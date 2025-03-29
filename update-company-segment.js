const axios = require('axios');
const fs = require('node:fs');
const path = require('node:path');
const csv = require('csv-parser');
const dotenv = require('dotenv');
const readline = require('node:readline');

// Load environment variables from .env file
dotenv.config();

// API configuration
const API_ENDPOINTS = (process.env.API_ENDPOINTS || 'http://localhost:8000,http://localhost:8001').split(',');
const BATCH_SIZE = Number.parseInt(process.env.BATCH_SIZE) || 10;
const DELAY_MS = Number.parseInt(process.env.DELAY_MS) || 2000;

/**
 * Reads company data from a CSV file
 */
function readCompanyDataFromCSV(filePath) {
    return new Promise((resolve, reject) => {
        try {
            console.log(`\nReading file: ${filePath}`);
            
            // Check if file exists
            if (!fs.existsSync(filePath)) {
                throw new Error(`File not found: ${filePath}`);
            }
            
            const results = [];
            
            fs.createReadStream(filePath)
                .pipe(csv())
                .on('data', (data) => {
                    // Convert and validate data
                    // Use 'id' or 'company_id' from CSV, depending on what's available
                    const companyId = Number(data.id || data.company_id);
                    const categoryValue = data.x_studio_categoria_economica;
                    
                    if (Number.isNaN(companyId)) {
                        console.warn(`Invalid id/company_id ignored: ${data.id || data.company_id}`);
                        return;
                    }
                    
                    results.push({
                        id: companyId,
                        x_studio_categoria_economica: categoryValue
                    });
                })
                .on('end', () => {
                    // Remove duplicates based on id
                    const uniqueCompanies = [];
                    const seen = new Set();
                    
                    for (const company of results) {
                        if (!seen.has(company.id)) {
                            seen.add(company.id);
                            uniqueCompanies.push(company);
                        }
                    }
                    
                    console.log(`Found ${uniqueCompanies.length} unique valid companies`);
                    resolve(uniqueCompanies);
                })
                .on('error', (error) => {
                    reject(new Error(`Error reading CSV: ${error.message}`));
                });
        } catch (error) {
            reject(error);
        }
    });
}

/**
 * Distributes requests across available endpoints
 */
function getEndpoint(index) {
    return API_ENDPOINTS[index % API_ENDPOINTS.length];
}

/**
 * Updates company segment with retry policy
 */
async function updateCompanyWithRetry(company, endpointIndex, retries = 3) {
    const endpoint = getEndpoint(endpointIndex);
    
    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            const response = await axios.patch(
                `${endpoint}/company/segment/${company.id}`,  // Using id parameter here
                {
                    x_studio_categoria_economica: company.x_studio_categoria_economica
                },
                {
                    headers: { 
                        'Content-Type': 'application/json'
                    },
                    timeout: 40000
                }
            );
            
            console.log(`✅ Success: Company ${company.id} updated via ${endpoint} (attempt ${attempt})`);
            return { 
                success: true, 
                data: response.data, 
                companyId: company.id,
                endpoint,
                fields: {
                    x_studio_categoria_economica: company.x_studio_categoria_economica
                }
            };
            
        } catch (error) {
            const errorMessage = error.response?.data?.detail || error.message;
            console.log(`❌ Error (attempt ${attempt}): Company ${company.id} via ${endpoint}: ${errorMessage}`);
            
            if (attempt === retries) {
                return {
                    success: false,
                    error: error.response?.data || error.message,
                    companyId: company.id,
                    endpoint,
                    fields: {
                        x_studio_categoria_economica: company.x_studio_categoria_economica
                    }
                };
            }
            
            // Exponential backoff between retries
            await new Promise(resolve => 
                setTimeout(resolve, 2000 * attempt)
            );
        }
    }
}

/**
 * Interactive confirmation interface
 */
async function confirmAction(question) {
    const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout
    });
    
    return new Promise(resolve => {
        rl.question(question, answer => {
            rl.close();
            resolve(answer.trim().toLowerCase() === 's');
        });
    });
}

/**
 * Test API endpoints connectivity
 */
async function testEndpoints() {
    console.log('\nTesting API endpoints connectivity...');
    
    const results = await Promise.all(
        API_ENDPOINTS.map(async (endpoint) => {
            try {
                // Try a simple GET request to test connectivity
                await axios.get(`${endpoint}`, { timeout: 10000 });
                return { endpoint, status: 'connected' };
            } catch (error) {
                // Specific error handling
                if (error.code === 'ECONNREFUSED') {
                    return { 
                        endpoint, 
                        status: 'error', 
                        message: 'Connection refused. Endpoint not available.' 
                    };
                } else if (error.response) {
                    // If we got any response, even an error, the endpoint is reachable
                    return { endpoint, status: 'connected' };
                } else {
                    return { 
                        endpoint, 
                        status: 'error', 
                        message: error.code || error.message 
                    };
                }
            }
        })
    );
    
    results.forEach(result => {
        if (result.status === 'connected') {
            console.log(`✅ Endpoint ${result.endpoint} is accessible`);
        } else {
            console.log(`❌ Endpoint ${result.endpoint} is not accessible: ${result.message}`);
        }
    });
    
    const workingEndpoints = results.filter(r => r.status === 'connected').map(r => r.endpoint);
    
    if (workingEndpoints.length === 0) {
        throw new Error('No API endpoints are accessible. Please check your configuration.');
    }
    
    return workingEndpoints;
}

/**
 * Main function
 */
async function main() {
    try {
        // Argument validation
        if (process.argv.length < 3) {
            console.log(`
            Usage: node ${path.basename(__filename)} <csv-file-path>
            
            Environment variables:
              API_ENDPOINTS: Comma-separated list of API endpoints (default: 'http://localhost:8000,http://localhost:32768')
              BATCH_SIZE: Number of requests to process in parallel (default: 5)
              DELAY_MS: Delay between batches in milliseconds (default: 1000)
            
            Examples:
              node update-company-segment.js data.csv
              node update-company-segment.js "C:\\Data\\companies.csv"
            `);
            process.exit(1);
        }
        
        // Test API endpoints and get working ones
        const workingEndpoints = await testEndpoints();
        
        // Override API_ENDPOINTS with only working ones
        if (workingEndpoints.length < API_ENDPOINTS.length) {
            console.log(`\n⚠️ Only ${workingEndpoints.length} out of ${API_ENDPOINTS.length} endpoints are accessible.`);
            console.log(`Using only these endpoints: ${workingEndpoints.join(', ')}`);
            API_ENDPOINTS.splice(0, API_ENDPOINTS.length, ...workingEndpoints);
        }
        
        const csvPath = process.argv[2];
        const companyData = await readCompanyDataFromCSV(csvPath);
        
        // Display configuration
        console.log(`\nConfiguration:`);
        console.log(`- API endpoints: ${API_ENDPOINTS.join(', ')}`);
        console.log(`- Batch size: ${BATCH_SIZE}`);
        console.log(`- Delay between batches: ${DELAY_MS}ms`);
        
        // Interactive confirmation
        if (!await confirmAction(
            `\n⚠️  ATTENTION: ${companyData.length} companies will have their segment updated across ${API_ENDPOINTS.length} endpoints.\n` +
            `Do you want to continue? (s/n) `
        )) {
            console.log('Operation cancelled by user');
            process.exit(0);
        }
        
        // Batch processing
        const results = { success: [], failed: [] };
        let processed = 0;
        
        console.log('\nStarting processing...');
        while (processed < companyData.length) {
            const batch = companyData.slice(processed, processed + BATCH_SIZE);
            const batchResults = await Promise.all(
                batch.map((company, index) => 
                    updateCompanyWithRetry(company, processed + index)
                )
            );
            
            batchResults.forEach(result => {
                result.success 
                    ? results.success.push(result)
                    : results.failed.push(result);
            });
            
            processed += batch.length;
            console.log(`Progress: ${processed}/${companyData.length} (${Math.round((processed/companyData.length)*100)}%)`);
            
            // Interval between batches
            if (processed < companyData.length) {
                await new Promise(resolve => setTimeout(resolve, DELAY_MS));
            }
        }
        
        // Report generation
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        const logData = {
            timestamp,
            config: {
                api_endpoints: API_ENDPOINTS,
                batch_size: BATCH_SIZE,
                delay_ms: DELAY_MS
            },
            stats: {
                total: companyData.length,
                success: results.success.length,
                failed: results.failed.length,
                distribution: API_ENDPOINTS.reduce((acc, endpoint) => {
                    acc[endpoint] = results.success.filter(r => r.endpoint === endpoint).length;
                    return acc;
                }, {})
            },
            failed: results.failed.map(f => ({
                company_id: f.companyId,
                endpoint: f.endpoint,
                fields: f.fields,
                error: f.error
            }))
        };
        
        // Save log
        const logPath = path.join(__dirname, `update-company-log-${timestamp}.json`);
        fs.writeFileSync(logPath, JSON.stringify(logData, null, 2));
        
        // Final summary
        console.log(`
        ========= FINAL REPORT =========
        Processed:    ${logData.stats.total}
        Successes:    ${logData.stats.success}
        Failures:     ${logData.stats.failed}
        
        Distribution by endpoint:
        ${Object.entries(logData.stats.distribution)
            .map(([endpoint, count]) => `- ${endpoint}: ${count}`)
            .join('\n        ')}
        
        Log file:     ${logPath}
        `);
        
        if (logData.stats.failed > 0) {
            console.log('Companies with failures:');
            results.failed.forEach(f => {
                console.log(`- Company ${f.companyId} (via ${f.endpoint})`);
            });
        }
        
    } catch (error) {
        console.error('\nCritical error:', error.message);
        process.exit(1);
    }
}

// Program execution
main();