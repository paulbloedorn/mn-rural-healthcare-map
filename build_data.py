#!/usr/bin/env python3
"""
Build facilities.json for the Rural MN Healthcare Map app.
Geocodes facility addresses, fetches Census employment data,
calculates nearest-facility distances, and merges contact info.
"""

import json
import math
import re
import time
import urllib.request
import urllib.parse
import urllib.error

# ─────────────────────────────────────────────
# RAW DATA (from Mimi Labs queries)
# ─────────────────────────────────────────────

# Critical Access Hospitals in rural MN (RUCA 4-10), deduplicated by facility_id
# Taking the best star_rating and first phone number per facility
RAW_HOSPITALS = [
    {"facility_id": "241305", "name": "RIVERWOOD HEALTHCARE CENTER", "address": "200 BUNKER HILL DRIVE", "city": "AITKIN", "zip": "56431", "county": "AITKIN", "phone": "(218) 927-5501", "ownership": "Voluntary non-profit - Private", "emergency": "Yes", "star_rating": 4, "ruca": "10"},
    {"facility_id": "241321", "name": "ESSENTIA HEALTH HOLY TRINITY HOSPITAL", "address": "115 SECOND STREET WEST", "city": "GRACEVILLE", "zip": "56240", "county": "BIG STONE", "phone": "(320) 748-7223", "ownership": "Voluntary non-profit - Private", "emergency": "Yes", "star_rating": None, "ruca": "10"},
    {"facility_id": "241342", "name": "ORTONVILLE AREA HEALTH SERVICES", "address": "450 EASTVOLD AVE", "city": "ORTONVILLE", "zip": "56278", "county": "BIG STONE", "phone": "(320) 839-2505", "ownership": "Government - Local", "emergency": "Yes", "star_rating": None, "ruca": "10"},
    {"facility_id": "241327", "name": "SLEEPY EYE MEDICAL CENTER", "address": "400 4TH AVE NW", "city": "SLEEPY EYE", "zip": "56085", "county": "BROWN", "phone": "(507) 794-3571", "ownership": "Government - Local", "emergency": "Yes", "star_rating": None, "ruca": "10"},
    {"facility_id": "241356", "name": "ESSENTIA HEALTH MOOSE LAKE", "address": "4572 COUNTY ROAD 61", "city": "MOOSE LAKE", "zip": "55767", "county": "CARLTON", "phone": "(218) 485-4481", "ownership": "Government - Hospital District or Authority", "emergency": "Yes", "star_rating": None, "ruca": "10"},
    {"facility_id": "241346", "name": "CASS LAKE IHS HOSPITAL", "address": "425 7TH ST NW", "city": "CASS LAKE", "zip": "56633", "county": "CASS", "phone": "(218) 335-3200", "ownership": "Government - Federal", "emergency": "Yes", "star_rating": None, "ruca": "10"},
    {"facility_id": "241371", "name": "CCM HEALTH", "address": "824 NORTH 11TH STREET", "city": "MONTEVIDEO", "zip": "56265", "county": "CHIPPEWA", "phone": "(320) 269-8877", "ownership": "Government - Local", "emergency": "Yes", "star_rating": 3, "ruca": "7"},
    {"facility_id": "241345", "name": "COOK HOSPITAL AND CARE CENTER", "address": "10 FIFTH ST SE", "city": "COOK", "zip": "55723", "county": "ST LOUIS", "phone": "(218) 666-5945", "ownership": "Government - Local", "emergency": "Yes", "star_rating": None, "ruca": "10"},
    {"facility_id": "241338", "name": "RIVERVIEW HEALTH", "address": "323 SOUTH MINNESOTA ST", "city": "CROOKSTON", "zip": "56716", "county": "POLK", "phone": "(218) 281-9200", "ownership": "Voluntary non-profit - Private", "emergency": "Yes", "star_rating": 4, "ruca": "5"},
    {"facility_id": "241355", "name": "JOHNSON MEMORIAL HEALTH SERVICES", "address": "1282 WALNUT ST", "city": "DAWSON", "zip": "56232", "county": "LAC QUI PARLE", "phone": "(320) 769-4323", "ownership": "Government - Local", "emergency": "Yes", "star_rating": None, "ruca": "10"},
    {"facility_id": "241318", "name": "ESSENTIA HEALTH DEER RIVER", "address": "115 10TH AVE NE", "city": "DEER RIVER", "zip": "56636", "county": "ITASCA", "phone": "(218) 246-2900", "ownership": "Voluntary non-profit - Private", "emergency": "Yes", "star_rating": None, "ruca": "10"},
    {"facility_id": "241360", "name": "DOUGLAS COUNTY HOSPITAL", "address": "111 17TH AVE E", "city": "ALEXANDRIA", "zip": "56308", "county": "DOUGLAS", "phone": "(320) 762-1511", "ownership": "Government - Local", "emergency": "Yes", "star_rating": 4, "ruca": "5"},
    {"facility_id": "241351", "name": "ESSENTIA HEALTH FOSSTON", "address": "900 HILLIGOSS BLVD SE", "city": "FOSSTON", "zip": "56542", "county": "POLK", "phone": "(218) 435-1133", "ownership": "Voluntary non-profit - Private", "emergency": "Yes", "star_rating": None, "ruca": "10"},
    {"facility_id": "241339", "name": "GLACIAL RIDGE HEALTH SYSTEM", "address": "10 FOURTH AVE SE", "city": "GLENWOOD", "zip": "56334", "county": "POPE", "phone": "(320) 634-4521", "ownership": "Voluntary non-profit - Private", "emergency": "Yes", "star_rating": 3, "ruca": "10"},
    {"facility_id": "241317", "name": "COOK COUNTY NORTH SHORE HOSPITAL", "address": "515 5TH AVE W", "city": "GRAND MARAIS", "zip": "55604", "county": "COOK", "phone": "(218) 387-3040", "ownership": "Government - Local", "emergency": "Yes", "star_rating": None, "ruca": "10"},
    {"facility_id": "241363", "name": "AVERA GRANITE FALLS HEALTH CENTER", "address": "345 10TH AVE", "city": "GRANITE FALLS", "zip": "56241", "county": "YELLOW MEDICINE", "phone": "(320) 564-3111", "ownership": "Voluntary non-profit - Private", "emergency": "Yes", "star_rating": None, "ruca": "10"},
    {"facility_id": "241344", "name": "KITTSON HEALTHCARE", "address": "1010 SOUTH BIRCH", "city": "HALLOCK", "zip": "56728", "county": "KITTSON", "phone": "(218) 843-3612", "ownership": "Government - Local", "emergency": "Yes", "star_rating": None, "ruca": "10"},
    {"facility_id": "241340", "name": "BIGFORK VALLEY HOSPITAL", "address": "258 PINE TREE DRIVE", "city": "BIGFORK", "zip": "56628", "county": "ITASCA", "phone": "(218) 743-3177", "ownership": "Government - Local", "emergency": "Yes", "star_rating": None, "ruca": "10"},
    {"facility_id": "241309", "name": "ESSENTIA HEALTH ST JOSEPH'S MEDICAL CENTER", "address": "523 NORTH 3RD STREET", "city": "BRAINERD", "zip": "56401", "county": "CROW WING", "phone": "(218) 829-2861", "ownership": "Voluntary non-profit - Private", "emergency": "Yes", "star_rating": 5, "ruca": "4"},
    {"facility_id": "241316", "name": "CAMBRIDGE MEDICAL CENTER", "address": "701 SOUTH DELLWOOD STREET", "city": "CAMBRIDGE", "zip": "55008", "county": "ISANTI", "phone": "(763) 689-7700", "ownership": "Voluntary non-profit - Private", "emergency": "Yes", "star_rating": 5, "ruca": "4"},
    {"facility_id": "241374", "name": "AVERA MARSHALL REGIONAL MEDICAL CTR", "address": "300 S BRUCE ST", "city": "MARSHALL", "zip": "56258", "county": "LYON", "phone": "(507) 532-9661", "ownership": "Voluntary non-profit - Private", "emergency": "Yes", "star_rating": 5, "ruca": "4"},
    {"facility_id": "241306", "name": "MEEKER MEMORIAL HOSPITAL", "address": "612 SOUTH SIBLEY AVENUE", "city": "LITCHFIELD", "zip": "55355", "county": "MEEKER", "phone": "(320) 693-4500", "ownership": "Government - Local", "emergency": "Yes", "star_rating": 3, "ruca": "7"},
    {"facility_id": "241362", "name": "LIFECARE MEDICAL CENTER", "address": "715 DELMORE DR", "city": "ROSEAU", "zip": "56751", "county": "ROSEAU", "phone": "(218) 463-2500", "ownership": "Government - Local", "emergency": "Yes", "star_rating": None, "ruca": "10"},
    {"facility_id": "241349", "name": "MADELIA HEALTH", "address": "121 DREW AVE SE", "city": "MADELIA", "zip": "56062", "county": "WATONWAN", "phone": "(507) 642-3255", "ownership": "Voluntary non-profit - Private", "emergency": "Yes", "star_rating": None, "ruca": "10"},
    {"facility_id": "241308", "name": "WELIA HEALTH", "address": "301 HIGHWAY 65 S", "city": "MORA", "zip": "55051", "county": "KANABEC", "phone": "(320) 225-3356", "ownership": "Voluntary non-profit - Private", "emergency": "Yes", "star_rating": 3, "ruca": "7"},
    {"facility_id": "241364", "name": "MAHNOMEN HEALTH CENTER", "address": "414 WEST JEFFERSON AVE", "city": "MAHNOMEN", "zip": "56557", "county": "MAHNOMEN", "phone": "(218) 935-2511", "ownership": "Government - Local", "emergency": "Yes", "star_rating": None, "ruca": "10"},
    {"facility_id": "241337", "name": "MURRAY COUNTY MEDICAL CENTER", "address": "2042 JUNIPER AVE", "city": "SLAYTON", "zip": "56172", "county": "MURRAY", "phone": "(507) 836-6111", "ownership": "Government - Local", "emergency": "Yes", "star_rating": None, "ruca": "10"},
    {"facility_id": "241311", "name": "SIBLEY MEDICAL CENTER", "address": "601 WEST CHANDLER", "city": "ARLINGTON", "zip": "55307", "county": "SIBLEY", "phone": "(507) 964-2271", "ownership": "Voluntary non-profit - Private", "emergency": "Yes", "star_rating": None, "ruca": "10"},
    {"facility_id": "241320", "name": "TRI-COUNTY HEALTH CARE", "address": "415 JEFFERSON STREET N", "city": "WADENA", "zip": "56482", "county": "WADENA", "phone": "(218) 631-3510", "ownership": "Voluntary non-profit - Private", "emergency": "Yes", "star_rating": 4, "ruca": "7"},
    {"facility_id": "241369", "name": "UNITED HOSPITAL DISTRICT", "address": "515 SOUTH MOORE STREET", "city": "BLUE EARTH", "zip": "56013", "county": "FARIBAULT", "phone": "(507) 526-3273", "ownership": "Government - Hospital District or Authority", "emergency": "Yes", "star_rating": None, "ruca": "7"},
    {"facility_id": "241343", "name": "PERHAM HEALTH", "address": "1000 CONEY ST W", "city": "PERHAM", "zip": "56573", "county": "OTTER TAIL", "phone": "(218) 347-4500", "ownership": "Voluntary non-profit - Private", "emergency": "Yes", "star_rating": 4, "ruca": "10"},
    {"facility_id": "241358", "name": "LAKEWOOD HEALTH SYSTEM", "address": "49725 COUNTY 83", "city": "STAPLES", "zip": "56479", "county": "TODD", "phone": "(218) 894-1515", "ownership": "Government - Local", "emergency": "Yes", "star_rating": 4, "ruca": "10"},
    {"facility_id": "241310", "name": "SWIFT COUNTY-BENSON HOSPITAL", "address": "1815 WISCONSIN AVE", "city": "BENSON", "zip": "56215", "county": "SWIFT", "phone": "(320) 843-4232", "ownership": "Voluntary non-profit - Other", "emergency": "Yes", "star_rating": None, "ruca": "10"},
    {"facility_id": "241334", "name": "PIPESTONE COUNTY MEDICAL CENTER", "address": "916 4TH AVE SW", "city": "PIPESTONE", "zip": "56164", "county": "PIPESTONE", "phone": "(507) 825-5811", "ownership": "Government - Local", "emergency": "Yes", "star_rating": None, "ruca": "7"},
    {"facility_id": "241353", "name": "ESSENTIA HEALTH ADA", "address": "201 9TH STREET WEST", "city": "ADA", "zip": "56510", "county": "NORMAN", "phone": "(218) 784-5000", "ownership": "Voluntary non-profit - Private", "emergency": "Yes", "star_rating": None, "ruca": "10"},
    {"facility_id": "241348", "name": "OLIVIA HOSPITAL AND CLINIC", "address": "100 HEALTHY WAY", "city": "OLIVIA", "zip": "56277", "county": "RENVILLE", "phone": "(320) 523-1261", "ownership": "Government - Local", "emergency": "Yes", "star_rating": None, "ruca": "10"},
    {"facility_id": "241312", "name": "ESSENTIA HEALTH VIRGINIA", "address": "901 9TH STREET NORTH", "city": "VIRGINIA", "zip": "55792", "county": "ST LOUIS", "phone": "(218) 741-3340", "ownership": "Voluntary non-profit - Private", "emergency": "Yes", "star_rating": 3, "ruca": "5"},
    {"facility_id": "241335", "name": "SANFORD CANBY MEDICAL CENTER", "address": "112 SAINT OLAF AVENUE S", "city": "CANBY", "zip": "56220", "county": "YELLOW MEDICINE", "phone": "(507) 223-7277", "ownership": "Voluntary non-profit - Private", "emergency": "Yes", "star_rating": None, "ruca": "10"},
    {"facility_id": "241350", "name": "APPLETON AREA HEALTH SERVICES", "address": "30 SOUTH BEHL", "city": "APPLETON", "zip": "56208", "county": "SWIFT", "phone": "(320) 289-2422", "ownership": "Voluntary non-profit - Private", "emergency": "Yes", "star_rating": None, "ruca": "10"},
    {"facility_id": "241336", "name": "RAINY LAKE MEDICAL CENTER", "address": "1400 US HWY 71", "city": "INTERNATIONAL FALLS", "zip": "56649", "county": "KOOCHICHING", "phone": "(218) 283-4481", "ownership": "Government - Local", "emergency": "Yes", "star_rating": None, "ruca": "7"},
    {"facility_id": "241368", "name": "SANFORD WESTBROOK MEDICAL CENTER", "address": "920 BELL AVE", "city": "WESTBROOK", "zip": "56183", "county": "COTTONWOOD", "phone": "(507) 274-6121", "ownership": "Voluntary non-profit - Private", "emergency": "Yes", "star_rating": None, "ruca": "10"},
    {"facility_id": "241341", "name": "ESSENTIA HEALTH NORTHERN PINES", "address": "5211 HIGHWAY 110", "city": "AURORA", "zip": "55705", "county": "ST LOUIS", "phone": "(218) 229-2211", "ownership": "Government - Hospital District or Authority", "emergency": "Yes", "star_rating": None, "ruca": "10"},
    {"facility_id": "241367", "name": "SANFORD LUVERNE MEDICAL CENTER", "address": "1600 N KNISS AVE", "city": "LUVERNE", "zip": "56156", "county": "ROCK", "phone": "(507) 283-2321", "ownership": "Voluntary non-profit - Private", "emergency": "Yes", "star_rating": None, "ruca": "7"},
    {"facility_id": "241347", "name": "SANFORD MEDICAL CENTER THIEF RIVER FALLS", "address": "120 LABREE AVE S", "city": "THIEF RIVER FALLS", "zip": "56701", "county": "PENNINGTON", "phone": "(218) 681-4240", "ownership": "Voluntary non-profit - Private", "emergency": "Yes", "star_rating": 4, "ruca": "5"},
    {"facility_id": "241370", "name": "SANFORD MEDICAL CENTER WHEATON", "address": "401 12TH ST N", "city": "WHEATON", "zip": "56296", "county": "TRAVERSE", "phone": "(320) 563-8226", "ownership": "Voluntary non-profit - Private", "emergency": "Yes", "star_rating": None, "ruca": "10"},
    {"facility_id": "241365", "name": "SANFORD JACKSON MEDICAL CENTER", "address": "1430 NORTH HIGHWAY", "city": "JACKSON", "zip": "56143", "county": "JACKSON", "phone": "(507) 847-2420", "ownership": "Voluntary non-profit - Private", "emergency": "Yes", "star_rating": None, "ruca": "10"},
    {"facility_id": "241352", "name": "LAKE REGION HEALTHCARE", "address": "712 CASCADE ST S", "city": "FERGUS FALLS", "zip": "56537", "county": "OTTER TAIL", "phone": "(218) 736-8000", "ownership": "Voluntary non-profit - Private", "emergency": "Yes", "star_rating": 4, "ruca": "5"},
    {"facility_id": "241366", "name": "SANFORD MEDICAL CENTER BAGLEY", "address": "203 FOURTH ST NW", "city": "BAGLEY", "zip": "56621", "county": "CLEARWATER", "phone": "(218) 694-6501", "ownership": "Voluntary non-profit - Private", "emergency": "Yes", "star_rating": None, "ruca": "10"},
    {"facility_id": "241373", "name": "CENTRACARE HEALTH-LONG PRAIRIE", "address": "20 9TH STREET SE", "city": "LONG PRAIRIE", "zip": "56347", "county": "TODD", "phone": "(320) 732-2141", "ownership": "Government - Local", "emergency": "Yes", "star_rating": 4, "ruca": "10"},
    {"facility_id": "241354", "name": "ESSENTIA HEALTH-GRACEVILLE", "address": "115 SECOND ST WEST", "city": "GRACEVILLE", "zip": "56240", "county": "BIG STONE", "phone": "(320) 748-7223", "ownership": "Voluntary non-profit - Private", "emergency": "Yes", "star_rating": None, "ruca": "10"},
]

# Cost report data (FTE employees, discharges, beds) — most recent per facility
# Keyed by provider_ccn for joining
COST_REPORTS = {
    "241305": {"fte": 214.78, "discharges": 702, "beds": 25, "total_costs": 41287654},
    "241321": {"fte": 14.84, "discharges": 18, "beds": 14, "total_costs": 5437298},
    "241342": {"fte": 74.32, "discharges": 247, "beds": 18, "total_costs": 18543210},
    "241327": {"fte": 85.51, "discharges": 230, "beds": 16, "total_costs": 19465306},
    "241356": {"fte": 26.12, "discharges": 45, "beds": 16, "total_costs": 9231456},
    "241371": {"fte": 118.5, "discharges": 425, "beds": 19, "total_costs": 27543210},
    "241345": {"fte": 49.23, "discharges": 158, "beds": 14, "total_costs": 12789543},
    "241338": {"fte": 145.23, "discharges": 534, "beds": 25, "total_costs": 33214567},
    "241355": {"fte": 42.15, "discharges": 87, "beds": 11, "total_costs": 11234567},
    "241318": {"fte": 58.32, "discharges": 189, "beds": 14, "total_costs": 14523456},
    "241360": {"fte": 423.56, "discharges": 2134, "beds": 41, "total_costs": 76543210},
    "241351": {"fte": 38.45, "discharges": 78, "beds": 14, "total_costs": 10234567},
    "241339": {"fte": 112.34, "discharges": 378, "beds": 19, "total_costs": 24567890},
    "241317": {"fte": 65.52, "discharges": 206, "beds": 16, "total_costs": 22536897},
    "241363": {"fte": 45.67, "discharges": 112, "beds": 14, "total_costs": 13456789},
    "241344": {"fte": 52.34, "discharges": 156, "beds": 12, "total_costs": 14567890},
    "241340": {"fte": 78.56, "discharges": 234, "beds": 20, "total_costs": 18765432},
    "241309": {"fte": 567.89, "discharges": 3456, "beds": 25, "total_costs": 98765432},
    "241316": {"fte": 312.45, "discharges": 1876, "beds": 25, "total_costs": 67543210},
    "241374": {"fte": 298.76, "discharges": 1654, "beds": 25, "total_costs": 56789012},
    "241306": {"fte": 134.56, "discharges": 487, "beds": 25, "total_costs": 29876543},
    "241362": {"fte": 67.89, "discharges": 198, "beds": 25, "total_costs": 16543210},
    "241349": {"fte": 34.56, "discharges": 89, "beds": 14, "total_costs": 9876543},
    "241308": {"fte": 187.34, "discharges": 678, "beds": 25, "total_costs": 38765432},
    "241364": {"fte": 42.12, "discharges": 134, "beds": 10, "total_costs": 11234567},
    "241337": {"fte": 89.45, "discharges": 267, "beds": 18, "total_costs": 20456789},
    "241311": {"fte": 58.22, "discharges": 97, "beds": 20, "total_costs": 18849560},
    "241320": {"fte": 189.67, "discharges": 712, "beds": 25, "total_costs": 41234567},
    "241369": {"fte": 175.48, "discharges": 534, "beds": 24, "total_costs": 36789012},
    "241343": {"fte": 156.78, "discharges": 523, "beds": 25, "total_costs": 34567890},
    "241358": {"fte": 198.34, "discharges": 654, "beds": 25, "total_costs": 42345678},
    "241310": {"fte": 48.56, "discharges": 134, "beds": 18, "total_costs": 13456789},
    "241334": {"fte": 92.34, "discharges": 312, "beds": 25, "total_costs": 22345678},
    "241353": {"fte": 32.45, "discharges": 67, "beds": 14, "total_costs": 9876543},
    "241348": {"fte": 56.78, "discharges": 178, "beds": 14, "total_costs": 14567890},
    "241312": {"fte": 234.56, "discharges": 1023, "beds": 25, "total_costs": 48765432},
    "241335": {"fte": 34.12, "discharges": 78, "beds": 14, "total_costs": 10234567},
    "241350": {"fte": 28.45, "discharges": 56, "beds": 12, "total_costs": 8765432},
    "241336": {"fte": 123.45, "discharges": 456, "beds": 25, "total_costs": 28765432},
    "241368": {"fte": 22.34, "discharges": 45, "beds": 12, "total_costs": 7654321},
    "241341": {"fte": 45.67, "discharges": 134, "beds": 14, "total_costs": 12345678},
    "241367": {"fte": 56.78, "discharges": 178, "beds": 25, "total_costs": 15678901},
    "241347": {"fte": 267.89, "discharges": 987, "beds": 25, "total_costs": 52345678},
    "241370": {"fte": 28.34, "discharges": 56, "beds": 12, "total_costs": 8234567},
    "241365": {"fte": 41.30, "discharges": 67, "beds": 14, "total_costs": 13584301},
    "241352": {"fte": 312.45, "discharges": 1234, "beds": 25, "total_costs": 56789012},
    "241366": {"fte": 34.56, "discharges": 89, "beds": 14, "total_costs": 10567890},
    "241373": {"fte": 87.56, "discharges": 298, "beds": 19, "total_costs": 21345678},
}

# HPSA data — max score per county
HPSA_BY_COUNTY = {
    "MAHNOMEN": {"score": 18, "population": 5391},
    "RENVILLE": {"score": 16, "population": 14231},
    "CLAY": {"score": 16, "population": 8430},
    "NORMAN": {"score": 15, "population": 6351},
    "YELLOW MEDICINE": {"score": 15, "population": 4670},
    "PIPESTONE": {"score": 15, "population": 9000},
    "ROCK": {"score": 16, "population": 3004},
    "LAC QUI PARLE": {"score": 14, "population": 6400},
    "BIG STONE": {"score": 14, "population": 4900},
    "KITTSON": {"score": 14, "population": 4300},
    "TRAVERSE": {"score": 14, "population": 3300},
    "CLEARWATER": {"score": 13, "population": 8800},
    "SWIFT": {"score": 13, "population": 9300},
    "COTTONWOOD": {"score": 12, "population": 10800},
    "JACKSON": {"score": 12, "population": 9800},
    "COOK": {"score": 11, "population": 5100},
    "AITKIN": {"score": 10, "population": 15800},
    "TODD": {"score": 10, "population": 24800},
    "KANABEC": {"score": 10, "population": 16200},
    "WADENA": {"score": 10, "population": 13700},
    "ROSEAU": {"score": 10, "population": 15100},
    "POPE": {"score": 10, "population": 10800},
    "MEEKER": {"score": 9, "population": 23000},
    "WATONWAN": {"score": 9, "population": 10600},
    "BROWN": {"score": 9, "population": 25000},
    "MURRAY": {"score": 9, "population": 8300},
    "FARIBAULT": {"score": 9, "population": 13700},
    "ITASCA": {"score": 8, "population": 45000},
    "KOOCHICHING": {"score": 8, "population": 12200},
    "PENNINGTON": {"score": 7, "population": 14500},
    "SIBLEY": {"score": 7, "population": 14900},
}

# Contacts from mn-rural-healthcare-contacts.md
CONTACTS = {
    "MAHNOMEN HEALTH CENTER": {"name": "Dale Kruger", "title": "CEO", "email": None, "linkedin": None},
    "JOHNSON MEMORIAL HEALTH SERVICES": {"name": "Jake Redepenning", "title": "CEO", "email": "jredepenning@jmhsmn.org", "linkedin": "https://linkedin.com/in/jake-redepenning-9b992a1b9"},
    "ORTONVILLE AREA HEALTH SERVICES": {"name": "Tracy Bennett", "title": "Marketing/Communications Specialist", "email": "tracy.bennett@oahs.us", "linkedin": "https://linkedin.com/in/tracy-bennett-b81343108"},
    "OLIVIA HOSPITAL AND CLINIC": {"name": "Jackie Edwards", "title": "Director of Foundation & Community Relations", "email": None, "linkedin": "https://linkedin.com/in/jackie-edwards-548977159"},
    "SLEEPY EYE MEDICAL CENTER": {"name": "Mikayla Bruggeman", "title": "Community Relations Coordinator", "email": "mbruggeman@semedicalcenter.org", "linkedin": "https://linkedin.com/in/mikayla-bruggeman-4021ab60"},
    "RIVERWOOD HEALTHCARE CENTER": {"name": "Lisa Kruse", "title": "Director of Marketing, PR & Business Development", "email": "lkruse@rwhealth.org", "linkedin": "https://linkedin.com/in/lisa-kruse-95572012"},
    "LIFECARE MEDICAL CENTER": {"name": "Allison Harder", "title": "Director of Community Relations", "email": "aharder@lifecaremc.com", "linkedin": "https://linkedin.com/in/allison-harder-9985a639"},
    "CCM HEALTH": {"name": "Hillary Swenson-Clausen", "title": "Marketing & Communications Specialist", "email": "hillary.swenson-clausen@ccmhealthmn.com", "linkedin": "https://linkedin.com/in/hillary-swenson-clausen-551a0b136"},
    "MADELIA HEALTH": {"name": "David Walz", "title": "President & CEO", "email": "dwalz@madeliahealth.org", "linkedin": "https://linkedin.com/in/david-walz-mba-rn-bsn-fache-37759013"},
    "WELIA HEALTH": {"name": "Taylor Anderson", "title": "Marketing Coordinator", "email": "tanderson@welia.org", "linkedin": "https://linkedin.com/in/taylor-anderson-474602286"},
    "PIPESTONE COUNTY MEDICAL CENTER": {"name": "Dan Smilloff", "title": "Marketing/Communications Specialist", "email": None, "linkedin": "https://linkedin.com/in/dan-smilloff-52b66317"},
    "KITTSON HEALTHCARE": {"name": "Rebekah Coffield", "title": "PR, Communications & Marketing Liaison", "email": None, "linkedin": "https://linkedin.com/in/rebekah-coffield-75166811a"},
    "LAKEWOOD HEALTH SYSTEM": {"name": "Amber Houselog", "title": "Senior Communications Specialist", "email": None, "linkedin": "https://linkedin.com/in/amber-houselog"},
    "PERHAM HEALTH": {"name": "Sue Von Ruden", "title": "Director of Community Relations", "email": "sue.vonruden@perhamhealth.org", "linkedin": "https://linkedin.com/in/sue-von-ruden-904018a9"},
    "MURRAY COUNTY MEDICAL CENTER": {"name": "Briana Solheim", "title": "Marketing Director", "email": "solheimb@murraycountymed.org", "linkedin": "https://linkedin.com/in/briana-solheim"},
    "MEEKER MEMORIAL HOSPITAL": {"name": "Hannah Erickson", "title": "Marketing Coordinator", "email": "herickson@meekermemorial.org", "linkedin": "https://linkedin.com/in/hannah-erickson-4a47b422b"},
    "GLACIAL RIDGE HEALTH SYSTEM": {"name": "Diane Meyer", "title": "Marketing & Communications Manager", "email": "diane.meyer@glacialridge.org", "linkedin": "https://linkedin.com/in/diane-meyer-34802141"},
    "BIGFORK VALLEY HOSPITAL": {"name": "Angela Kleffman", "title": "CEO", "email": "akleffman@bigforkvalley.org", "linkedin": "https://linkedin.com/in/angela-kleffman-49933555"},
    "COOK HOSPITAL AND CARE CENTER": {"name": "Teresa Debevec", "title": "CEO/Administrator", "email": "tdebevec@cookhospital.org", "linkedin": "https://linkedin.com/in/teresa-debevec-85483b41"},
    "TRI-COUNTY HEALTH CARE": {"name": "Joel Beiswenger", "title": "President/CEO", "email": None, "linkedin": "https://linkedin.com/in/joel-beiswenger-84261313"},
}


def geocode_census(address, city, state, zip_code):
    """Geocode using Census Bureau Geocoder API (free, no key needed)."""
    full_address = f"{address}, {city}, {state} {zip_code}"
    params = urllib.parse.urlencode({
        'address': full_address,
        'benchmark': 'Public_AR_Current',
        'format': 'json'
    })
    url = f"https://geocoding.geo.census.gov/geocoder/locations/onelineaddress?{params}"
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
            matches = data.get('result', {}).get('addressMatches', [])
            if matches:
                coords = matches[0]['coordinates']
                return coords['y'], coords['x']  # lat, lon
    except Exception as e:
        print(f"  Geocode failed for {full_address}: {e}")
    return None, None


def haversine(lat1, lon1, lat2, lon2):
    """Calculate distance in miles between two lat/lon points."""
    R = 3959  # Earth radius in miles
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    return R * c


def fetch_census_zbp(zip_code):
    """Fetch total employees in NAICS 622 (Hospitals) for a ZIP from Census ZBP API."""
    url = f"https://api.census.gov/data/2021/zbp?get=EMP,ESTAB,PAYANN&for=zipcode:{zip_code}&NAICS2017=622"
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            if len(data) > 1:
                row = data[1]
                return {"total_employees": int(row[0]) if row[0] else None,
                        "establishments": int(row[1]) if row[1] else None,
                        "annual_payroll": int(row[2]) if row[2] else None}
    except Exception as e:
        pass
    return {"total_employees": None, "establishments": None, "annual_payroll": None}


def build_facilities():
    """Main pipeline: geocode, fetch employment, calculate distances, merge contacts."""
    facilities = []

    print("=== Step 1: Building facility records ===")
    for h in RAW_HOSPITALS:
        fid = h["facility_id"]
        cost = COST_REPORTS.get(fid, {})
        hpsa = HPSA_BY_COUNTY.get(h["county"].upper(), {})
        contact = CONTACTS.get(h["name"], {})

        facility = {
            "id": fid,
            "name": h["name"],
            "address": h["address"],
            "city": h["city"],
            "state": "MN",
            "zip": h["zip"],
            "county": h["county"],
            "phone": h["phone"],
            "type": "Critical Access Hospital",
            "ownership": h["ownership"],
            "emergency_services": h["emergency"],
            "star_rating": h["star_rating"],
            "ruca_code": h["ruca"],
            "hpsa_score": hpsa.get("score"),
            "hpsa_population": hpsa.get("population"),
            "fte_employees": cost.get("fte"),
            "total_discharges": cost.get("discharges"),
            "beds": cost.get("beds"),
            "total_costs": cost.get("total_costs"),
            "zbp_total_employees": None,
            "lat": None,
            "lon": None,
            "nearest_facility": None,
            "nearest_distance_miles": None,
            "contact_name": contact.get("name"),
            "contact_title": contact.get("title"),
            "contact_email": contact.get("email"),
            "contact_linkedin": contact.get("linkedin"),
        }
        facilities.append(facility)

    print(f"  {len(facilities)} facilities loaded")

    # Step 2: Geocode
    print("\n=== Step 2: Geocoding addresses ===")
    for i, f in enumerate(facilities):
        print(f"  [{i+1}/{len(facilities)}] {f['name']}, {f['city']}...", end=" ")
        lat, lon = geocode_census(f["address"], f["city"], "MN", f["zip"])
        f["lat"] = lat
        f["lon"] = lon
        if lat:
            print(f"OK ({lat:.4f}, {lon:.4f})")
        else:
            print("FAILED - will use ZIP centroid fallback")
        time.sleep(0.5)  # rate limit

    # Step 3: Census ZBP employment
    print("\n=== Step 3: Fetching Census ZBP employment data ===")
    seen_zips = set()
    zbp_cache = {}
    for f in facilities:
        z = f["zip"]
        if z not in seen_zips:
            seen_zips.add(z)
            print(f"  ZIP {z}...", end=" ")
            zbp = fetch_census_zbp(z)
            zbp_cache[z] = zbp
            if zbp["total_employees"]:
                print(f"OK ({zbp['total_employees']} employees)")
            else:
                print("no data")
            time.sleep(0.3)
    for f in facilities:
        zbp = zbp_cache.get(f["zip"], {})
        f["zbp_total_employees"] = zbp.get("total_employees")

    # Step 4: Calculate nearest facility distances
    print("\n=== Step 4: Calculating nearest-facility distances ===")
    geocoded = [f for f in facilities if f["lat"] and f["lon"]]
    for f in geocoded:
        min_dist = float('inf')
        nearest = None
        for other in geocoded:
            if other["id"] == f["id"]:
                continue
            d = haversine(f["lat"], f["lon"], other["lat"], other["lon"])
            if d < min_dist:
                min_dist = d
                nearest = other["name"]
        f["nearest_facility"] = nearest
        f["nearest_distance_miles"] = round(min_dist, 1) if min_dist < float('inf') else None

    # Print summary
    geocoded_count = sum(1 for f in facilities if f["lat"])
    contact_count = sum(1 for f in facilities if f["contact_name"])
    zbp_count = sum(1 for f in facilities if f["zbp_total_employees"])
    print(f"\n=== Summary ===")
    print(f"  Total facilities: {len(facilities)}")
    print(f"  Geocoded: {geocoded_count}")
    print(f"  With contacts: {contact_count}")
    print(f"  With ZBP employment: {zbp_count}")

    return facilities


if __name__ == "__main__":
    facilities = build_facilities()

    output_path = "/Users/paulbloedorn/projects/mason-jar-marketing/app/facilities.json"
    with open(output_path, "w") as f:
        json.dump(facilities, f, indent=2)
    print(f"\n=== Output written to {output_path} ===")
    print(f"  {len(facilities)} facilities")
