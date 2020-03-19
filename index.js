const csv = require('csv-parser');
const fs = require('fs');
const glob = require('glob');
const _ = require('lodash');


const dir = process.argv.slice(2)[0] || __dirname;

const results = {};

glob(dir + '/*.CSV', {}, (err, files) => {
	let doneCount = 0;
	const prepareCSV = (arr) => {
		return arr
			.map((line) => line.join(','))
			.join('\n');
	}
	const done = () => {
		++doneCount;
		if (doneCount === files.length) {
			const resultArr = prepareResults(results);

			const sourceFileNames = files.map((fileName) => fileName.split('/').pop().replace('.CSV', ''))
			const outputFileName = `result ${sourceFileNames.join(' vs ')}.csv`;
			fs.writeFile(outputFileName, prepareCSV(resultArr), (err) => {
				if (err) throw err;
				console.log(`File "${outputFileName}" has been saved!`);
			});
		}
	};

	files.forEach((fileName) => {
		results[fileName] = {};

		fs.createReadStream(fileName)
			.pipe(csv(['date', 'time', 'open', 'high', 'low', 'close', 'tick_volume']))
			.on('data', (row) => {
				results[fileName][`${row.date}-${row.time}`] = row.close;
			})
			.on('end', () => {
				
				console.log('CSV file successfully processed');

				done();

			});
	})
});


function prepareResults(resultsObj) {

	const fileNames = Object.keys(resultsObj);

	const intersection = intersect(...fileNames.map((fileName) => Object.keys(resultsObj[fileName]) ));

	return [
		['time', ...fileNames.map((fileName) => fileName.split('/').pop())],
		...intersection.map((dateTime) => {
			return [dateTime, ...fileNames.map((fileName) => resultsObj[fileName][dateTime])];
		})
	]
}



function intersect(a, b) { // NOTE: if there are more than two files, it will work for two first
	var setA = new Set(a);
	var setB = new Set(b);
	var intersection = new Set([...setA].filter(x => setB.has(x)));
	return Array.from(intersection);
}