(function (global) {
	"use strict";

	const ColorService = {
		palettes: {
			"Tableau.Classic10": [
				"#4E79A7",
				"#F28E2B",
				"#E15759",
				"#76B7B2",
				"#59A14F",
				"#EDC949",
				"#AF7AA1",
				"#FF9DA7",
				"#9C755F",
				"#BAB0AB",
			],
			"brewer.Pastel1-9": [
				"#FBB4AE",
				"#B3CDE3",
				"#CCEBC5",
				"#DECBE4",
				"#FED9A6",
				"#FFFFCC",
				"#E5D8BD",
				"#FDDAEC",
				"#F2F2F2",
			],
			"office.Office6": [
				"#4472C4",
				"#ED7D31",
				"#A5A5A5",
				"#FFC000",
				"#5B9BD5",
				"#70AD47",
			],
			"tableau.Tableau20": [
				"#4E79A7",
				"#A0CBE8",
				"#F28E2B",
				"#FFBE7D",
				"#59A14F",
				"#8CD17D",
				"#B6992D",
				"#F1CE63",
				"#499894",
				"#86BCB6",
				"#E15759",
				"#FF9D9A",
				"#79706E",
				"#BAB0AC",
				"#D37295",
				"#FABFD2",
				"#B07AA1",
				"#D4A6C8",
				"#9D7660",
				"#D7B5A6",
			],
		},

		getColors(paletteName = "Tableau.Classic10") {
			return this.palettes[paletteName] || this.palettes["Tableau.Classic10"];
		},
	};

	// This line makes the ColorService available to all other scripts
	global.ColorService = ColorService;
})(window);
