/**
 * @description
 *    Mapbiomas land use/land cover classification for Brazil
 *    This dataset provides information on land use and land cover changes over time.
 *    It is part of the Mapbiomas initiative, which aims to monitor and analyze land use dynamics in Brazil.
 *    The dataset is updated regularly to reflect the latest changes in land use and land cover.
 * @author
 *    João Siqueira
 * 
 */

// Asset mapbiomas
var asset = "projects/mapbiomas-public/assets/brazil/lulc/collection10/mapbiomas_brazil_collection10_coverage_v2";

var palettes = require('users/mapbiomas/modules:Palettes.js');

var palette = palettes.get('brazil');

var image = ee.Image(asset);

Map.addLayer(image, {
    bands: ['classification_2024'], 
    min: 0, 
    max: 75, 
    palette: palette, 
    format: 'png'
}, 
'Mapbiomas 2024');