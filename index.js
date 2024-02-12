// const {BigQuery} = require('@google-cloud/bigquery');
// const bq = new BigQuery();
// const datasetId = "weather_etl"
// const tableId = "demo"

// const demoCode =()=>{

// fakeObject = {};
// fakeObject.first_name = "Ahmed";
// fakeObject.last_name = "Mohammed";
// fakeObject.email ="ahmoha@iu.edu";
// fakeObject.age = 28;
// writeToBq(fakeObject);
// }

// demoCode();

// //create a help function that writes to BQ

// async function writeToBq(obj){
// //BQ expects an array of objects, but this function only receives 1

// var rows=[];
// rows.push(obj);
// await bq
// .dataset(datasetId)
// .table(tableId)
// .insert(rows)
// .then (()=> {
//     rows.forEach ((row)=>{console.log(`inserted: ${row}`)})
// })
// .catch ((err)=>{console.error(`ERROR: ${err}`)})
// }



const {BigQuery} = require('@google-cloud/bigquery');
const {Storage} = require('@google-cloud/storage');
const csv = require('csv-parser');

const gcs = new Storage();
const bq = new BigQuery();

const datasetId = "weather_etl";
const tableId = "weatherindy";

exports.transferFive = (file, context) => {
    const dataFile = gcs.bucket(file.bucket).file(file.name);
    let rows = []; 
    dataFile.createReadStream()
    .pipe(csv())
    .on('data', (row) => {
        const transformedRow = transformRow(row, file.name);
        if (transformedRow) { 
            rows.push(transformedRow); }
        })

        .on('end', async () => {
            if (rows.length > 0) {
                await writeToBq(rows).catch(err => console.error('ERROR:', err));
            }
            console.log('COMPLETED:');
        })};
        function transformRow(row, fileName) {
        
            const stationIdPattern = /\d{6}-\d{5}/;
            const stationIdMatch = fileName.match(stationIdPattern);
            const stationId = stationIdMatch ? stationIdMatch[0] : null;
            row.station = stationId;
            const floating = ['airtemp', 'dewpoint', 'pressure', 'windspeed', 'precip1hour', 'precip6hour'];
        
            Object.keys(row).forEach(key => {
                const value = parseInt(row[key],10)
                
                if (value === -9999) {
                    row[key] = null; 
                } else if (floating.includes(key)) {
                    row[key] = parseFloat(row[key]) / 10;
                } 
            });
        
            return row;
        }
        async function writeToBq(rows) {
            await bq.dataset(datasetId).table(tableId).insert(rows);
        }