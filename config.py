BASE_URL = "https://critmin.org"
POLICY_URL = "https://critmin.org/policies/"
YEARS = range(1970, 2024)
MAX_WORKERS = 6
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30
OUTPUT_DIR = "output"
OUTPUT_TABLE = "YOUR_PROJECT.YOUR_DATASET.YOUR_TABLE"

countries = [
    'AFG', 'ALB', 'DZA', 'AND', 'AGO', 'ATG', 'ARG',
    'ARM', 'ABW', 'AUS', 'AUT', 'AZE', 'BHS', 'BHR',
    'BGD', 'BRB', 'BLR', 'BEL', 'BLZ', 'BEN', 'BMU',
    'BTN', 'BOL', 'BIH', 'BWA', 'BRA', 'BRN', 'BGR',
    'BFA', 'BDI', 'CPV', 'KHM', 'CMR', 'CAN', 'CYM',
    'CAF', 'TCD', 'CHL', 'COL', 'COM', 'COG', 'COK',
    'CRI', 'CIV', 'HRV', 'CUB', 'CUW', 'CYP', 'CZE',
    'COD', 'DNK', 'DJI', 'DMA', 'DOM', 'ECU', 'EGY',
    'SLV', 'GNQ', 'EST', 'SWZ', 'ETH', 'FSM', 'FJI',
    'FIN', 'FRA', 'PYF', 'GAB', 'GEO', 'DEU', 'GHA',
    'GRC', 'GRL', 'GRD', 'GTM', 'GIN', 'GNB', 'GUY',
    'HTI', 'VAT', 'HND', 'HKG', 'HUN', 'ISL', 'IND',
    'IDN', 'IRN', 'IRQ', 'IRL', 'ISR', 'ITA', 'JAM',
    'JPN', 'JOR', 'KAZ', 'KEN', 'KIR', 'KWT', 'KGZ',
    'LAO', 'LVA', 'LBN', 'LSO', 'LBR', 'LBY', 'LIE',
    'LTU', 'LUX', 'MAC', 'MDG', 'MWI', 'MYS', 'MDV',
    'MLI', 'MLT', 'MRT', 'MUS', 'MEX', 'MDA', 'MNG',
    'MNE', 'MSR', 'MAR', 'MOZ', 'MMR', 'NAM', 'NPL',
    'NLD', 'NCL', 'NZL', 'NIC', 'NER', 'NGA', 'MKD',
    'NOR', 'OMN', 'PAK', 'PLW', 'PSE', 'PAN', 'PNG',
    'PRY', 'CHN', 'PER', 'PHL', 'POL', 'PRT', 'QAT',
    'KOR', 'ROM', 'RUS', 'RWA', 'KNA', 'LCA', 'VCT',
    'WSM', 'STP', 'SAU', 'SEN', 'SRB', 'SYC', 'SLE',
    'SGP', 'SVK', 'SVN', 'SLB', 'SOM', 'ZAF', 'SSD',
    'ESP', 'LKA', 'SDN', 'SUR', 'SWE', 'CHE', 'SYR',
    'CHT', 'TJK', 'TZA', 'THA', 'GMB', 'TLS', 'TGO',
    'TON', 'TTO', 'TUN', 'TUR', 'TKM', 'TCA', 'UGA',
    'UKR', 'ARE', 'GBR', 'USA', 'XKX', 'URY', 'UZB',
    'VUT', 'VEN', 'VNM', 'YEM', 'ZMB', 'ZWE'
]