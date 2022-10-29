// Taken from node-pg package

const pad = (number: number | string, digits: number) => {
	number = "" + number;
	while (number.length < digits) number = "0" + number;
	return number;
};

const dateToString = (date: Date): string => {
	let offset = -date.getTimezoneOffset();
	let year = date.getFullYear();
	const isBCYear = year < 1;
	if (isBCYear) year = Math.abs(year) + 1;

	let ret =
		pad(year, 4) +
		"-" +
		pad(date.getMonth() + 1, 2) +
		"-" +
		pad(date.getDate(), 2) +
		"T" +
		pad(date.getHours(), 2) +
		":" +
		pad(date.getMinutes(), 2) +
		":" +
		pad(date.getSeconds(), 2) +
		"." +
		pad(date.getMilliseconds(), 3);

	if (offset < 0) {
		ret += "-";
		offset *= -1;
	} else {
		ret += "+";
	}

	ret += pad(Math.floor(offset / 60), 2) + ":" + pad(offset % 60, 2);
	if (isBCYear) ret += " BC";
	return ret;
};

export default dateToString;
