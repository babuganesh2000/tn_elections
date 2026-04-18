"""
Tamil Nadu Elections Intelligence App - Data Generator v2
Covers: 2001, 2006, 2011, 2016, 2021 (historical) + 2026 (nominations + swing)

Calibrated to known ECI outcomes:
  2001: AIADMK alliance wins (Jayalalithaa returns, ~196 seats)
  2006: DMK alliance wins (Karunanidhi, ~163 seats)
  2011: AIADMK sweep (Jayalalithaa, ~203 seats)
  2016: AIADMK wins (Jayalalithaa, ~136 seats)
  2021: DMK+INDIA Alliance landslide (M.K. Stalin, ~133+ seats)

2026 corrections:
  - Four-corner structure: DMK alliance, AIADMK alliance, NTK, TVK
  - NTK and TVK field candidates in all 234 constituencies
  - DMK and AIADMK blocs distribute seats among alliance partners
  - Kamal Haasan NOT contesting (MNM dissolved)
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path

PARQUET_DIR = Path("data/parquet")
PARQUET_DIR.mkdir(parents=True, exist_ok=True)
SOURCE_METADATA_PATH = PARQUET_DIR / "source_metadata.json"
rng = np.random.default_rng(42)

KNOWN_OUTCOMES = {
    2001: {"AIADMK_ALLIANCE": 196, "DMK_ALLIANCE": 34,  "others": 4},
    2006: {"DMK_ALLIANCE":    163, "AIADMK_ALLIANCE": 69, "others": 2},
    2011: {"AIADMK_ALLIANCE": 203, "DMK_ALLIANCE": 23,  "others": 8},
    2016: {"AIADMK_ALLIANCE": 136, "DMK_ALLIANCE": 98,  "others": 0},
    2021: {"DMK_ALLIANCE":    159, "AIADMK_ALLIANCE": 75, "others": 0},
}
KNOWN_TURNOUT = {
    2001: (60, 68), 2006: (62, 70), 2011: (75, 84), 2016: (73, 81), 2021: (70, 80),
}

CONSTITUENCIES = [
    (1,"Gummidipoondi","Thiruvallur","GEN","North"),(2,"Ponneri","Thiruvallur","SC","North"),
    (3,"Tiruttani","Thiruvallur","GEN","North"),(4,"Sholinghur","Vellore","GEN","North"),
    (5,"Katpadi","Vellore","GEN","North"),(6,"Ranipet","Ranipet","GEN","North"),
    (7,"Arcot","Ranipet","GEN","North"),(8,"Vellore","Vellore","GEN","North"),
    (9,"Anaikattu","Vellore","SC","North"),(10,"Kilvaithinankuppam","Vellore","SC","North"),
    (11,"Gudiyatham","Vellore","GEN","North"),(12,"Vaniyambadi","Tirupathur","GEN","North"),
    (13,"Ambur","Tirupathur","GEN","North"),(14,"Jolarpet","Tirupathur","GEN","North"),
    (15,"Tirupathur","Tirupathur","GEN","North"),(16,"Uthangarai","Krishnagiri","ST","West"),
    (17,"Bargur","Krishnagiri","GEN","West"),(18,"Krishnagiri","Krishnagiri","GEN","West"),
    (19,"Veppanahalli","Krishnagiri","GEN","West"),(20,"Hosur","Krishnagiri","GEN","West"),
    (21,"Thalli","Krishnagiri","SC","West"),(22,"Anchetty","Krishnagiri","ST","West"),
    (23,"Denkanikottai","Krishnagiri","GEN","West"),(24,"Pennagaram","Dharmapuri","ST","West"),
    (25,"Dharmapuri","Dharmapuri","GEN","West"),(26,"Pappireddipatti","Dharmapuri","GEN","West"),
    (27,"Harur","Dharmapuri","ST","West"),(28,"Tiruvannamalai","Tiruvannamalai","GEN","North"),
    (29,"Kilpennathur","Tiruvannamalai","GEN","North"),(30,"Polur","Tiruvannamalai","SC","North"),
    (31,"Arani","Tiruvannamalai","SC","North"),(32,"Cheyyar","Tiruvannamalai","GEN","North"),
    (33,"Vandavasi","Tiruvannamalai","GEN","North"),(34,"Gingee","Villupuram","GEN","Central"),
    (35,"Mailam","Villupuram","SC","Central"),(36,"Tindivanam","Villupuram","GEN","Central"),
    (37,"Vanur","Villupuram","SC","Central"),(38,"Villupuram","Villupuram","GEN","Central"),
    (39,"Vikravandi","Villupuram","GEN","Central"),(40,"Tirukoilur","Villupuram","GEN","Central"),
    (41,"Ulundurpet","Villupuram","GEN","Central"),(42,"Rishivandiyam","Villupuram","SC","Central"),
    (43,"Sankarapuram","Villupuram","GEN","Central"),(44,"Kallakurichi","Kallakurichi","GEN","Central"),
    (45,"Gangavalli","Salem","GEN","West"),(46,"Attur","Salem","GEN","West"),
    (47,"Yercaud","Salem","ST","West"),(48,"Omalur","Salem","GEN","West"),
    (49,"Mettur","Salem","GEN","West"),(50,"Edappadi","Salem","GEN","West"),
    (51,"Salem West","Salem","GEN","West"),(52,"Salem North","Salem","GEN","West"),
    (53,"Salem South","Salem","SC","West"),(54,"Veerapandi","Salem","GEN","West"),
    (55,"Rasipuram","Namakkal","GEN","West"),(56,"Senthamangalam","Namakkal","ST","West"),
    (57,"Namakkal","Namakkal","GEN","West"),(58,"Paramathi-Velur","Namakkal","GEN","West"),
    (59,"Tiruchengode","Namakkal","GEN","West"),(60,"Kumarapalayam","Namakkal","GEN","West"),
    (61,"Erode East","Erode","GEN","West"),(62,"Erode West","Erode","GEN","West"),
    (63,"Modakurichi","Erode","GEN","West"),(64,"Perundurai","Erode","GEN","West"),
    (65,"Bhavani","Erode","GEN","West"),(66,"Anthiyur","Erode","GEN","West"),
    (67,"Gobichettipalayam","Erode","GEN","West"),(68,"Bhavanisagar","Erode","SC","West"),
    (69,"Sathyamangalam","Erode","ST","West"),(70,"Nambiyur","Erode","ST","West"),
    (71,"Gudalur","Nilgiris","ST","West"),(72,"Udhagamandalam","Nilgiris","GEN","West"),
    (73,"Coonoor","Nilgiris","GEN","West"),(74,"Coimbatore North","Coimbatore","GEN","West"),
    (75,"Thondamuthur","Coimbatore","GEN","West"),(76,"Coimbatore South","Coimbatore","GEN","West"),
    (77,"Singanallur","Coimbatore","GEN","West"),(78,"Kinathukadavu","Coimbatore","GEN","West"),
    (79,"Pollachi","Coimbatore","GEN","West"),(80,"Valparai","Coimbatore","SC","West"),
    (81,"Sulur","Coimbatore","GEN","West"),(82,"Kavundampalayam","Coimbatore","GEN","West"),
    (83,"Mettupalayam","Coimbatore","GEN","West"),(84,"Tiruppur North","Tiruppur","GEN","West"),
    (85,"Tiruppur South","Tiruppur","GEN","West"),(86,"Palladam","Tiruppur","GEN","West"),
    (87,"Dharapuram","Tiruppur","GEN","West"),(88,"Kangeyam","Tiruppur","GEN","West"),
    (89,"Avinashi","Tiruppur","GEN","West"),(90,"Udumalpet","Tiruppur","GEN","West"),
    (91,"Madurai North","Madurai","GEN","South"),(92,"Madurai South","Madurai","GEN","South"),
    (93,"Madurai Central","Madurai","GEN","South"),(94,"Madurai East","Madurai","GEN","South"),
    (95,"Madurai West","Madurai","GEN","South"),(96,"Sholavandan","Madurai","SC","South"),
    (97,"Thirparappu","Madurai","GEN","South"),(98,"Melur","Madurai","GEN","South"),
    (99,"Thiruparankundram","Madurai","GEN","South"),(100,"Tirumangalam","Madurai","GEN","South"),
    (101,"Usilampatti","Madurai","GEN","South"),(102,"Andipatti","Theni","GEN","South"),
    (103,"Periyakulam","Theni","GEN","South"),(104,"Bodinayakanur","Theni","GEN","South"),
    (105,"Theni","Theni","GEN","South"),(106,"Rajapalayam","Virudhunagar","GEN","South"),
    (107,"Srivilliputtur","Virudhunagar","SC","South"),(108,"Sattur","Virudhunagar","GEN","South"),
    (109,"Sivakasi","Virudhunagar","GEN","South"),(110,"Virudhunagar","Virudhunagar","GEN","South"),
    (111,"Aruppukkottai","Virudhunagar","GEN","South"),(112,"Tiruchuli","Virudhunagar","GEN","South"),
    (113,"Thanjavur","Thanjavur","GEN","Delta"),(114,"Orathanadu","Thanjavur","GEN","Delta"),
    (115,"Papanasam","Thanjavur","GEN","Delta"),(116,"Thiruvaiyaru","Thanjavur","GEN","Delta"),
    (117,"Kumbakonam","Thanjavur","GEN","Delta"),(118,"Pattukkottai","Thanjavur","SC","Delta"),
    (119,"Thiruvidaimarudur","Thanjavur","GEN","Delta"),(120,"Mayiladuthurai","Mayiladuthurai","GEN","Delta"),
    (121,"Sirkazhi","Nagapattinam","SC","Delta"),(122,"Chidambaram","Cuddalore","GEN","Delta"),
    (123,"Cuddalore","Cuddalore","GEN","Delta"),(124,"Bhuvanagiri","Cuddalore","GEN","Delta"),
    (125,"Panruti","Cuddalore","GEN","Delta"),(126,"Kurinjipadi","Cuddalore","SC","Delta"),
    (127,"Neyveli","Cuddalore","GEN","Delta"),(128,"Vridhachalam","Cuddalore","GEN","Delta"),
    (129,"Tiruchirappalli West","Tiruchirappalli","GEN","Central"),
    (130,"Tiruchirappalli East","Tiruchirappalli","GEN","Central"),
    (131,"Thiruverumbur","Tiruchirappalli","GEN","Central"),
    (132,"Srirangam","Tiruchirappalli","GEN","Central"),
    (133,"Lalgudi","Tiruchirappalli","SC","Central"),(134,"Manachanallur","Tiruchirappalli","GEN","Central"),
    (135,"Musiri","Tiruchirappalli","GEN","Central"),(136,"Thuraiyur","Tiruchirappalli","GEN","Central"),
    (137,"Perambalur","Perambalur","SC","Central"),(138,"Kunnam","Perambalur","GEN","Central"),
    (139,"Ariyalur","Ariyalur","GEN","Central"),(140,"Jayankondam","Ariyalur","SC","Central"),
    (141,"Gangaikondancholapuram","Ariyalur","GEN","Central"),
    (142,"Pudukottai","Pudukkottai","GEN","Central"),(143,"Thirumayam","Pudukkottai","SC","Central"),
    (144,"Aranthangi","Pudukkottai","GEN","Central"),(145,"Ganesh Nagar","Pudukkottai","GEN","Central"),
    (146,"Alangudi","Pudukkottai","GEN","Central"),(147,"Karaikudi","Sivaganga","GEN","South"),
    (148,"Manamadurai","Sivaganga","GEN","South"),(149,"Sivaganga","Sivaganga","GEN","South"),
    (150,"Vilavancode","Kanyakumari","GEN","South"),(151,"Dindigul","Dindigul","GEN","South"),
    (152,"Natham","Dindigul","GEN","South"),(153,"Vedasandur","Dindigul","GEN","South"),
    (154,"Aravakurichi","Karur","GEN","Central"),(155,"Karur","Karur","GEN","Central"),
    (156,"Krishnarayapuram","Karur","SC","Central"),(157,"Kulithalai","Karur","GEN","Central"),
    (158,"Manapparai","Tiruchirappalli","GEN","Central"),(159,"Palani","Dindigul","GEN","South"),
    (160,"Tirunelveli","Tirunelveli","GEN","South"),(161,"Ambasamudram","Tirunelveli","GEN","South"),
    (162,"Palayamkottai","Tirunelveli","GEN","South"),(163,"Nanguneri","Tirunelveli","SC","South"),
    (164,"Radhapuram","Tirunelveli","GEN","South"),(165,"Tenkasi","Tenkasi","GEN","South"),
    (166,"Alangulam","Tenkasi","GEN","South"),(167,"Kadayanallur","Tenkasi","SC","South"),
    (168,"Sankarankovil","Tenkasi","GEN","South"),(169,"Thoothukudi","Thoothukudi","GEN","South"),
    (170,"Tiruchendur","Thoothukudi","GEN","South"),(171,"Srivaikuntam","Thoothukudi","SC","South"),
    (172,"Ottapidaram","Thoothukudi","GEN","South"),(173,"Kovilpatti","Thoothukudi","GEN","South"),
    (174,"Vilathikulam","Thoothukudi","GEN","South"),(175,"Nagercoil","Kanyakumari","GEN","South"),
    (176,"Colachel","Kanyakumari","GEN","South"),(177,"Padmanabhapuram","Kanyakumari","GEN","South"),
    (178,"Killiyoor","Kanyakumari","GEN","South"),(179,"Nagercoil North","Kanyakumari","GEN","South"),
    (180,"Marthandam","Kanyakumari","GEN","South"),(181,"Harbour","Chennai","GEN","Chennai"),
    (182,"Chepauk-Thiruvallikeni","Chennai","GEN","Chennai"),
    (183,"Thousand Lights","Chennai","GEN","Chennai"),(184,"Anna Nagar","Chennai","GEN","Chennai"),
    (185,"Villivakkam","Chennai","GEN","Chennai"),(186,"Kolathur","Chennai","GEN","Chennai"),
    (187,"Perambur","Chennai","GEN","Chennai"),(188,"Egmore","Chennai","GEN","Chennai"),
    (189,"Royapuram","Chennai","SC","Chennai"),(190,"Thiru Vi Ka Nagar","Chennai","GEN","Chennai"),
    (191,"Tiruvottiyur","Chennai","GEN","Chennai"),
    (192,"Dr. Radhakrishnan Nagar","Chennai","GEN","Chennai"),
    (193,"Velachery","Chennai","GEN","Chennai"),(194,"Sholinganallur","Chennai","GEN","Chennai"),
    (195,"Alandur","Chennai","GEN","Chennai"),(196,"Saidapet","Chennai","GEN","Chennai"),
    (197,"Virugambakkam","Chennai","GEN","Chennai"),(198,"Mylapore","Chennai","GEN","Chennai"),
    (199,"Madambakkam","Chennai","GEN","Chennai"),(200,"Pallikaranai","Chennai","GEN","Chennai"),
    (201,"Ambattur","Chennai","GEN","Chennai"),(202,"Avadi","Thiruvallur","GEN","North"),
    (203,"Poonamallee","Thiruvallur","GEN","North"),(204,"Thiruverkadu","Thiruvallur","GEN","North"),
    (205,"Madhavaram","Chennai","SC","Chennai"),(206,"Thirumangalam","Chennai","GEN","Chennai"),
    (207,"Perungudi","Chennai","GEN","Chennai"),(208,"Thiyagarayanagar","Chennai","GEN","Chennai"),
    (209,"Guindy","Chennai","GEN","Chennai"),(210,"Harbour South","Chennai","GEN","Chennai"),
    (211,"Korattur","Chennai","GEN","Chennai"),(212,"Purasawalkam","Chennai","GEN","Chennai"),
    (213,"Kolathur North","Chennai","GEN","Chennai"),(214,"Villivakkam North","Chennai","GEN","Chennai"),
    (215,"Tondiarpet","Chennai","GEN","Chennai"),(216,"Washermanpet","Chennai","SC","Chennai"),
    (217,"Periamet","Chennai","GEN","Chennai"),(218,"Chennai Central","Chennai","GEN","Chennai"),
    (219,"Chengalpattu","Chengalpattu","GEN","South"),(220,"Cheyyur","Chengalpattu","SC","South"),
    (221,"Madurantakam","Chengalpattu","GEN","South"),(222,"Uthiramerur","Chengalpattu","GEN","South"),
    (223,"Kancheepuram","Kanchipuram","GEN","North"),(224,"Arakkonam","Vellore","SC","North"),
    (225,"Sholinghur North","Vellore","GEN","North"),(226,"Sriperumbudur","Kanchipuram","GEN","North"),
    (227,"Pallavaram","Kanchipuram","GEN","North"),(228,"Tambaram","Chengalpattu","GEN","South"),
    (229,"Chengalpattu North","Chengalpattu","GEN","South"),
    (230,"Thiruporur","Chengalpattu","GEN","South"),(231,"Vandalore","Chengalpattu","GEN","South"),
    (232,"Kovur","Chennai","SC","Chennai"),(233,"Kundrathur","Chennai","GEN","Chennai"),
    (234,"Maduravoyal","Chennai","GEN","Chennai"),
]

PARTIES = [
    (1,"Dravida Munnetra Kazhagam","DMK","INDIA Alliance",2021),
    (2,"All India Anna Dravida Munnetra Kazhagam","AIADMK","Independent",2021),
    (3,"Indian National Congress","INC","INDIA Alliance",2021),
    (4,"Bharatiya Janata Party","BJP","NDA",2021),
    (5,"Viduthalai Chiruthaigal Katchi","VCK","INDIA Alliance",2021),
    (6,"Communist Party of India (Marxist)","CPM","INDIA Alliance",2021),
    (7,"Communist Party of India","CPI","INDIA Alliance",2021),
    (8,"Marumalarchi Dravida Munnetra Kazhagam","MDMK","INDIA Alliance",2021),
    (9,"Indian Union Muslim League","IUML","INDIA Alliance",2021),
    (10,"Pattali Makkal Katchi","PMK","NDA",2021),
    (11,"Naam Tamilar Katchi","NTK","Independent",2021),
    (12,"All India Majlis-e-Ittehadul Muslimeen","AIMIM","Independent",2021),
    (13,"Tamil Maanila Congress","TMC(M)","INDIA Alliance",2021),
    (14,"Desiya Murpokku Dravida Kazhagam","DMDK","Independent",2016),
    (15,"Independent","IND","Independent",2001),
    (16,"Amma Makkal Munnetra Kazhagam","AMMK","Independent",2021),
    (17,"Tamilaga Vettri Kazhagam","TVK","Independent",2026),
    (18,"Makkal Needhi Maiam","MNM","Independent",2019),
    (19,"MGR Anna Dravida Munnetra Kazhagam","MGRK","Independent",2001),
]

REFERENCE_CANDIDATES = [
    ("T. M. Anbarasan", "M"),
    ("B. Valarmathi", "F"),
    ("Panruti S. Ramachandran", "M"),
    ("Dr. K. Ghayathri Devi", "F"),
    ("R. Vijayakumar", "M"),
    ("H. Raja", "M"),
    ("R. M. Veerappan", "M"),
    ("R. Lavakumar", "M"),
    ("Sarathbabu", "M"),
    ("Dr. R. Karthikeyan", "M"),
    ("S. Saravanan", "M"),
    ("Mahalakshmi", "F"),
    ("R. M. Harish", "M"),
    ("Dr. S. Sathyanarayanan", "M"),
    ("S. Sathya Narayanan", "M"),
    ("R. Srinivasan", "M"),
    ("Dhanachezhian", "M"),
    ("U. Chandran", "M"),
]

VERIFIED_CONSTITUENCY_RESULTS = {
    (195, 2001): [
        {"candidate_name": "B. Valarmathi", "party_id": 2, "votes": 94554, "vote_share": 47.59, "rank": 1, "winner_flag": True, "margin_votes": 12596, "turnout_pct": 46.96},
        {"candidate_name": "R. M. Veerappan", "party_id": 19, "votes": 81958, "vote_share": 41.25, "rank": 2, "winner_flag": False, "margin_votes": None, "turnout_pct": 46.96},
        {"candidate_name": "R. Lavakumar", "party_id": 8, "votes": 13440, "vote_share": 6.76, "rank": 3, "winner_flag": False, "margin_votes": None, "turnout_pct": 46.96},
    ],
    (195, 2006): [
        {"candidate_name": "T. M. Anbarasan", "party_id": 1, "votes": 133232, "vote_share": 46.85, "rank": 1, "winner_flag": True, "margin_votes": 17910, "turnout_pct": 65.84},
        {"candidate_name": "B. Valarmathi", "party_id": 2, "votes": 115322, "vote_share": 40.55, "rank": 2, "winner_flag": False, "margin_votes": None, "turnout_pct": 65.84},
        {"candidate_name": "R. Vijayakumar", "party_id": 14, "votes": 22866, "vote_share": 8.04, "rank": 3, "winner_flag": False, "margin_votes": None, "turnout_pct": 65.84},
        {"candidate_name": "H. Raja", "party_id": 4, "votes": 9298, "vote_share": 3.27, "rank": 4, "winner_flag": False, "margin_votes": None, "turnout_pct": 65.84},
    ],
    (195, 2011): [
        {"candidate_name": "Panruti S. Ramachandran", "party_id": 14, "votes": 76537, "vote_share": 45.52, "rank": 1, "winner_flag": True, "margin_votes": 5754, "turnout_pct": 70.07},
        {"candidate_name": "Dr. K. Ghayathri Devi", "party_id": 3, "votes": 70783, "vote_share": 42.10, "rank": 2, "winner_flag": False, "margin_votes": None, "turnout_pct": 70.07},
        {"candidate_name": "S. Sathya Narayanan", "party_id": 4, "votes": 9628, "vote_share": 5.73, "rank": 3, "winner_flag": False, "margin_votes": None, "turnout_pct": 70.07},
    ],
    (195, 2016): [
        {"candidate_name": "T. M. Anbarasan", "party_id": 1, "votes": 96877, "vote_share": 44.64, "rank": 1, "winner_flag": True, "margin_votes": 19169, "turnout_pct": 61.74},
        {"candidate_name": "Panruti S. Ramachandran", "party_id": 2, "votes": 77708, "vote_share": 35.81, "rank": 2, "winner_flag": False, "margin_votes": None, "turnout_pct": 61.74},
        {"candidate_name": "Dr. S. Sathyanarayanan", "party_id": 4, "votes": 12806, "vote_share": 5.90, "rank": 3, "winner_flag": False, "margin_votes": None, "turnout_pct": 61.74},
        {"candidate_name": "U. Chandran", "party_id": 14, "votes": 12291, "vote_share": 5.66, "rank": 4, "winner_flag": False, "margin_votes": None, "turnout_pct": 61.74},
        {"candidate_name": "R. Srinivasan", "party_id": 10, "votes": 7194, "vote_share": 3.32, "rank": 5, "winner_flag": False, "margin_votes": None, "turnout_pct": 61.74},
        {"candidate_name": "Dhanachezhian", "party_id": 11, "votes": 3927, "vote_share": 1.81, "rank": 6, "winner_flag": False, "margin_votes": None, "turnout_pct": 61.74},
    ],
    (195, 2021): [
        {"candidate_name": "T. M. Anbarasan", "party_id": 1, "votes": 116785, "vote_share": 49.12, "rank": 1, "winner_flag": True, "margin_votes": 40571, "turnout_pct": 61.11},
        {"candidate_name": "B. Valarmathi", "party_id": 2, "votes": 76214, "vote_share": 32.06, "rank": 2, "winner_flag": False, "margin_votes": None, "turnout_pct": 61.11},
        {"candidate_name": "Sarathbabu", "party_id": 18, "votes": 21139, "vote_share": 8.89, "rank": 3, "winner_flag": False, "margin_votes": None, "turnout_pct": 61.11},
        {"candidate_name": "Dr. R. Karthikeyan", "party_id": 11, "votes": 16522, "vote_share": 6.95, "rank": 4, "winner_flag": False, "margin_votes": None, "turnout_pct": 61.11},
    ],
}

VERIFIED_2026_NOMINATIONS = {
    195: [
        {"candidate_name": "T. M. Anbarasan", "party_id": 1, "final_contesting_flag": True},
        {"candidate_name": "S. Saravanan", "party_id": 2, "final_contesting_flag": True},
        {"candidate_name": "Mahalakshmi", "party_id": 11, "final_contesting_flag": True},
        {"candidate_name": "R. M. Harish", "party_id": 17, "final_contesting_flag": True},
    ]
}

DMK_2026_PARTIES = [1,3,5,6,7,8,9,13]
AIADMK_2026_PARTIES = [2,4,10,14,16]
DMK_2026_ALLOCATIONS = {1: 170, 3: 25, 5: 12, 6: 8, 7: 7, 8: 6, 9: 3, 13: 3}
AIADMK_2026_ALLOCATIONS = {2: 170, 4: 20, 10: 18, 14: 12, 16: 14}
PARTY_REGION_PRIORITY = {
    1: ["Chennai", "Delta", "Central", "South", "North", "West"],
    2: ["West", "South", "North", "Central", "Chennai", "Delta"],
    3: ["South", "Central", "North", "Delta", "Chennai", "West"],
    4: ["Chennai", "West", "North", "South", "Central", "Delta"],
    5: ["Delta", "Central", "North", "South", "Chennai", "West"],
    6: ["Central", "Delta", "Chennai", "North", "South", "West"],
    7: ["Central", "Delta", "North", "South", "Chennai", "West"],
    8: ["South", "Central", "Delta", "North", "West", "Chennai"],
    9: ["Delta", "Chennai", "Central", "South", "North", "West"],
    10: ["North", "Central", "West", "South", "Chennai", "Delta"],
    13: ["North", "Central", "Chennai", "South", "Delta", "West"],
    14: ["South", "West", "Central", "North", "Chennai", "Delta"],
    16: ["South", "West", "Central", "North", "Delta", "Chennai"],
}

# Corrected celebrity data - Kamal Haasan NOT in 2026
CELEBRITIES = [
    ("M.K. Stalin","M",True,"politician",
     "Chief Minister of Tamil Nadu (2021-); DMK President; son of M. Karunanidhi. Contesting Kolathur 2026.",
     "https://upload.wikimedia.org/wikipedia/commons/thumb/5/5a/MK_Stalin_2019.jpg/220px-MK_Stalin_2019.jpg"),
    ("Udhayanidhi Stalin","M",True,"politician",
     "Tamil Nadu Deputy Chief Minister; DMK youth leader; won Chepauk-Thiruvallikeni in 2021 and is modeled there again for 2026.",
     None),
    ("Edappadi K. Palaniswami","M",True,"politician",
     "Former CM (2017-2021); AIADMK General Secretary; Edappadi is his home constituency and fortress.",
     "https://upload.wikimedia.org/wikipedia/commons/thumb/5/5b/Edapaddi_k_palanisamy.jpg/220px-Edapaddi_k_palanisamy.jpg"),
    ("O. Panneerselvam","M",True,"politician",
     "Former CM (3 terms); Senior AIADMK leader; Base in Andipatti, Theni district.",
     "https://upload.wikimedia.org/wikipedia/commons/thumb/e/e4/O._Panneerselvam_2015.jpg/220px-O._Panneerselvam_2015.jpg"),
    ("Seeman","M",True,"actor/politician",
     "Actor and NTK (Naam Tamilar Katchi) founder; Contested 2011, 2016, 2021; 2026: Tirunelveli.",
     "https://upload.wikimedia.org/wikipedia/commons/thumb/0/04/Seeman_2017.jpg/220px-Seeman_2017.jpg"),
    ("Vijay","M",True,"actor",
     "Superstar actor; founder of Tamilaga Vettri Kazhagam (TVK); 2026 contest from Perambur and Tiruchirappalli East.",
     "https://upload.wikimedia.org/wikipedia/commons/thumb/3/38/Actor_Vijay_Stills_%281%29.jpg/220px-Actor_Vijay_Stills_%281%29.jpg"),
    ("Durai Murugan","M",True,"politician",
     "Senior DMK minister; Forest & Water Resources Min; Katpadi veteran; son-in-law tie to DMK inner circle.",
     "https://upload.wikimedia.org/wikipedia/commons/thumb/1/1d/DuraiMurugan.jpg/220px-DuraiMurugan.jpg"),
    ("Sarathkumar","M",True,"actor",
     "Actor-politician; AISMK founder; Nagercoil area; contested 2016, 2021; 2026 re-run expected.",
     "https://upload.wikimedia.org/wikipedia/commons/thumb/7/73/Sarath_kumar.jpg/220px-Sarath_kumar.jpg"),
    ("Khushbu Sundar","F",True,"actor",
     "Actress-turned-BJP National Secretary; high-profile Chennai face for BJP in 2021, but not a 2026 assembly candidate.",
     "https://upload.wikimedia.org/wikipedia/commons/thumb/e/e2/Kushboo_at_Vijay_Television_Awards_2014.jpg/220px-Kushboo_at_Vijay_Television_Awards_2014.jpg"),
    ("Tamilisai Soundararajan","F",True,"politician",
     "Former Governor of Telangana; BJP TN president; Key BJP face in Chennai for 2026.",
     "https://upload.wikimedia.org/wikipedia/commons/thumb/3/3f/Tamilisai_Soundararajan.jpg/220px-Tamilisai_Soundararajan.jpg"),
    ("Anbumani Ramadoss","M",True,"politician",
     "PMK President; former Union Health Minister; Rajya Sabha member from Tamil Nadu in 2026, not modeled here as an assembly contestant.",
     "https://upload.wikimedia.org/wikipedia/commons/thumb/d/d8/Anbumani_Ramadoss.jpg/220px-Anbumani_Ramadoss.jpg"),
    # Kamal Haasan - NOT contesting 2026
    ("Kamal Haasan","M",True,"actor",
     "Superstar actor; MNM founder. NOT contesting 2026 - MNM not fielding candidates; withdrew from active politics.",
     "https://upload.wikimedia.org/wikipedia/commons/thumb/8/8d/Kamal_Haasan_at_the_63rd_Filmfare_Awards_South.jpg/220px-Kamal_Haasan_at_the_63rd_Filmfare_Awards_South.jpg"),
    ("Jayalalithaa","F",True,"politician",
     "CM of TN (4 terms); AIADMK supremo; Led 2001 and 2011 sweeps. Deceased December 2016.",
     "https://upload.wikimedia.org/wikipedia/commons/thumb/6/64/Jayalalithaa_2015.jpg/220px-Jayalalithaa_2015.jpg"),
    ("M. Karunanidhi","M",True,"politician",
     "CM of TN (5 terms, 1969-2011 various); DMK patriarch; Led 2006 alliance win. Deceased August 2018.",
     "https://upload.wikimedia.org/wikipedia/commons/thumb/f/f5/Kalaignar_Karunanidhi.jpg/220px-Kalaignar_Karunanidhi.jpg"),
]

# Celebrity seat assignments
CELEB_SEATS = [
    (186,2001,True,"Kolathur - DMK bastion; senior DMK leader era of Karunanidhi","M. Karunanidhi","politician"),
    (186,2006,True,"Kolathur - DMK sweeps; Karunanidhi CM again","M. Karunanidhi","politician"),
    (102,2001,True,"Andipatti - O. Panneerselvam first major win; AIADMK Theni fortress","O. Panneerselvam","politician"),
    (102,2011,True,"Andipatti - OPS retains in 2011 AIADMK sweep","O. Panneerselvam","politician"),
    (102,2016,True,"Andipatti - OPS vs DMK close contest","O. Panneerselvam","politician"),
    (50, 2016,True,"Edappadi - EPS emerges as AIADMK heavyweight; his home seat","Edappadi K. Palaniswami","politician"),
    (50, 2021,True,"Edappadi - EPS contests as incumbent CM; AIADMK's prestige seat","Edappadi K. Palaniswami","politician"),
    (186,2021,True,"Kolathur - M.K. Stalin wins decisively; becomes CM of Tamil Nadu","M.K. Stalin","politician"),
    (182,2021,True,"Chepauk-Thiruvallikeni - Udhayanidhi Stalin wins for DMK in central Chennai","Udhayanidhi Stalin","politician"),
    (183,2021,True,"Thousand Lights - Khushbu Sundar BJP debut; urban Chennai showdown","Khushbu Sundar","actor"),
    (175,2021,True,"Nagercoil - Sarathkumar prominent AISMK contest in south TN","Sarathkumar","actor"),
    # 2026 - Kamal Haasan removed
    (186,2026,True,"Kolathur - M.K. Stalin re-contesting as incumbent CM (DMK)","M.K. Stalin","politician"),
    (182,2026,True,"Chepauk-Thiruvallikeni - Udhayanidhi Stalin is modeled as the DMK candidate for 2026","Udhayanidhi Stalin","politician"),
    (50, 2026,True,"Edappadi - EPS fortress; AIADMK's most prestigious 2026 seat","Edappadi K. Palaniswami","politician"),
    (187,2026,True,"Perambur - Vijay (TVK) contest; North Chennai urban test seat","Vijay","actor"),
    (130,2026,True,"Tiruchirappalli East - Vijay (TVK) second contest; central TN expansion seat","Vijay","actor"),
    (160,2026,True,"Tirunelveli - Seeman (NTK) urban south TN campaign seat","Seeman","actor/politician"),
    (183,2026,True,"Thousand Lights - Tamilisai Soundararajan (BJP) high-profile entry","Tamilisai Soundararajan","politician"),
    (175,2026,True,"Nagercoil - Sarathkumar re-contest; celebrity seat in Kanyakumari district","Sarathkumar","actor"),
]

def get_alliance(year):
    if year == 2001: return [1,3,6,7],[2,10],[11,14,15]
    if year == 2006: return [1,3,5,6,7,8,9],[2,10],[11,14,15]
    if year == 2011: return [1,3,5,6,7,8,9],[2,10,14],[11,15]
    if year == 2016: return [1,3,5,6,7,8,9,13],[2,10],[11,14,16,15]
    return [1,3,5,6,7,8,9,13],[2,10,16],[11,12,15]  # 2021

def build_dim_constituency():
    df = pd.DataFrame([{"constituency_id":c[0],"constituency_name":c[1],"district":c[2],"type":c[3],"region":c[4]} for c in CONSTITUENCIES])
    df.to_parquet(PARQUET_DIR/"dim_constituency.parquet",index=False)
    print(f"[ok] dim_constituency: {len(df)} rows"); return df

def build_dim_party():
    df = pd.DataFrame([{"party_id":p[0],"party_name":p[1],"party_abbr":p[2],"alliance_name":p[3],"alliance_year":p[4]} for p in PARTIES])
    df.to_parquet(PARQUET_DIR/"dim_party.parquet",index=False)
    print(f"[ok] dim_party: {len(df)} rows"); return df

def build_dim_candidate(n=2400):
    MALE=["Arumugam","Balakrishnan","Chandrasekaran","Durai","Elangovan","Ganesan","Hariharan","Ilangovan","Jayakumar","Karthikeyan","Loganathan","Murugesan","Natarajan","Palaniappan","Ramasamy","Selvam","Thangavel","Venkataraman","Arjunan","Balasubramanian","Chelladurai","Kumaresan","Krishnamurthy","Marimuthu","Narayanan","Pandi","Rajendran","Saravanan","Periasamy","Muthusamy","Rajenthiran","Swaminathan","Viswanathan","Gunasekaran","Jeyaraj","Kasinathan","Lingam","Murugan"]
    FEMALE=["Bharathi","Chitra","Dhanalakshmi","Ezhilarasi","Geetha","Hemalatha","Indirani","Jayanthi","Kavitha","Lalitha","Meena","Nirmala","Parvathy","Radha","Selvi","Tamilarasi","Uma","Vanitha","Arulmozhi","Kanimozhi","Malathi","Nagalakshmi","Padmavathi","Revathi","Subha","Ponni"]
    LAST=["Perumal","Pillai","Gounder","Nadar","Thevar","Mudaliar","Chettiar","Rajan","Kumar","Selvam","Mani","Das","Pandian","Azhagan","Sundaram","Raj","Vel","Nathan","Doss","Krishnan"]
    records=[]
    cid=1
    for nm,gn,cf,ct,notes,photo in CELEBRITIES:
        records.append({"candidate_id":cid,"candidate_name":nm,"gender":gn,"celebrity_flag":cf,"celebrity_type":ct,"profile_notes":notes,"photo_url":photo})
        cid+=1
    existing_names = {r["candidate_name"] for r in records}
    for nm, gn in REFERENCE_CANDIDATES:
        if nm in existing_names:
            continue
        records.append({"candidate_id":cid,"candidate_name":nm,"gender":gn,"celebrity_flag":False,"celebrity_type":None,"profile_notes":"Reference constituency candidate loaded from verified constituency override.","photo_url":None})
        cid+=1
    for _ in range(max(0, n-len(records))):
        isf=rng.random()<0.18
        records.append({"candidate_id":cid,"candidate_name":f"{rng.choice(FEMALE if isf else MALE)} {rng.choice(LAST)}","gender":"F" if isf else "M","celebrity_flag":False,"celebrity_type":None,"profile_notes":None,"photo_url":None})
        cid+=1
    df=pd.DataFrame(records)
    df.to_parquet(PARQUET_DIR/"dim_candidate.parquet",index=False)
    print(f"[ok] dim_candidate: {len(df)} rows"); return df


def assign_2026_alliance_seats(dc, allocations, locked=None):
    locked = locked or {}
    assigned = {}
    remaining_ids = [int(cid) for cid in dc["constituency_id"].tolist() if int(cid) not in locked]

    for cid, party_id in locked.items():
        assigned[int(cid)] = int(party_id)

    rows = dc.set_index("constituency_id")
    for party_id, count in allocations.items():
        count_to_fill = count - sum(1 for v in assigned.values() if v == party_id)
        if count_to_fill <= 0:
            continue

        priorities = PARTY_REGION_PRIORITY.get(party_id, [])
        scored = []
        for cid in remaining_ids:
            region = rows.loc[cid, "region"]
            try:
                priority_score = priorities.index(region)
            except ValueError:
                priority_score = len(priorities)
            scored.append((priority_score, rng.random(), cid))
        scored.sort()

        for _, _, cid in scored[:count_to_fill]:
            assigned[cid] = int(party_id)
            remaining_ids.remove(cid)

    return assigned

def build_fact_results(dc, dcand):
    all_cids=dcand["candidate_id"].tolist()
    candidate_lookup = dcand.set_index("candidate_name")["candidate_id"].to_dict()
    celeb_map=dcand[dcand["celebrity_flag"]==True].set_index("candidate_name")["candidate_id"].to_dict()
    celeb_seat_map={}
    for cs in CELEB_SEATS:
        if cs[1]!=2026: celeb_seat_map[(cs[0],cs[1])]=celeb_map.get(cs[4])
    records=[]
    for year in [2001,2006,2011,2016,2021]:
        outcomes=KNOWN_OUTCOMES[year]; total=sum(outcomes.values())
        dmk_win=outcomes.get("DMK_ALLIANCE",0)/total
        aiadmk_win=outcomes.get("AIADMK_ALLIANCE",0)/total
        tr=KNOWN_TURNOUT[year]
        dmk_p,aiadmk_p,other_p=get_alliance(year)
        used=set()
        for _,cr in dc.iterrows():
            cid=int(cr["constituency_id"]); n_c=int(rng.integers(5,13))
            override_rows = VERIFIED_CONSTITUENCY_RESULTS.get((cid, year))
            if override_rows:
                for row in override_rows:
                    cand_id = int(candidate_lookup[row["candidate_name"]])
                    used.add(cand_id)
                    records.append({
                        "election_year":year,
                        "constituency_id":cid,
                        "candidate_id":cand_id,
                        "party_id":int(row["party_id"]),
                        "votes":int(row["votes"]),
                        "vote_share":float(row["vote_share"]),
                        "rank":int(row["rank"]),
                        "winner_flag":bool(row["winner_flag"]),
                        "margin_votes":row["margin_votes"],
                        "turnout_pct":float(row["turnout_pct"]),
                    })
                continue
            r=rng.random()
            if r<dmk_win: wp=int(rng.choice(dmk_p))
            elif r<dmk_win+aiadmk_win: wp=int(rng.choice(aiadmk_p))
            else: wp=int(rng.choice(other_p))
            all_p=[wp]+[p for p in (dmk_p[:2]+aiadmk_p[:2]+other_p[:2]) if p!=wp][:n_c-1]
            while len(all_p)<n_c: all_p.append(15)
            alphas=np.ones(n_c); alphas[0]=5
            sh=rng.dirichlet(alphas)
            sh[0]=float(rng.uniform(0.36,0.57))
            sh[1:]=(1-sh[0])*sh[1:]/sh[1:].sum()
            sh=np.sort(sh)[::-1]
            electors=int(rng.integers(160000,310000))
            turnout=float(rng.uniform(tr[0]/100,tr[1]/100))
            total_v=int(electors*turnout)
            margin=int(sh[0]*total_v)-int(sh[1]*total_v)
            for rank,(s,pid) in enumerate(zip(sh,all_p),1):
                if rank==1 and (cid,year) in celeb_seat_map and celeb_seat_map[(cid,year)]:
                    cand_id=celeb_seat_map[(cid,year)]
                else:
                    avail=[c for c in all_cids if c not in used] or all_cids
                    cand_id=int(rng.choice(avail[:600] if len(avail)>600 else avail))
                used.add(cand_id)
                records.append({"election_year":year,"constituency_id":cid,"candidate_id":cand_id,"party_id":int(pid),"votes":int(s*total_v),"vote_share":round(float(s)*100,2),"rank":rank,"winner_flag":rank==1,"margin_votes":margin if rank==1 else None,"turnout_pct":round(turnout*100,2)})
    df=pd.DataFrame(records)
    df.to_parquet(PARQUET_DIR/"fact_results.parquet",index=False)
    print(f"[ok] fact_results: {len(df)} rows (5 elections: 2001-2021)"); return df

def build_fact_nominations_2026(dc, dcand):
    all_cids=dcand["candidate_id"].tolist()
    candidate_lookup = dcand.set_index("candidate_name")["candidate_id"].to_dict()
    celeb_map=dcand[dcand["celebrity_flag"]==True].set_index("candidate_name")["candidate_id"].to_dict()
    all_celeb_ids=set(dcand[dcand["celebrity_flag"]==True]["candidate_id"].tolist())  # exclude from random pool

    # Kamal Haasan (ID=11) is NOT contesting 2026 - MNM not fielding candidates
    # Vijay contests from Perambur and Tiruchirappalli East under TVK (party_id=17)
    celeb_2026={
        187:(celeb_map.get("Vijay"), 17),          # TVK
        130:(celeb_map.get("Vijay"), 17),          # TVK
        186:(celeb_map.get("M.K. Stalin"), 1),      # DMK
        182:(celeb_map.get("Udhayanidhi Stalin"), 1),  # DMK
        50: (celeb_map.get("Edappadi K. Palaniswami"), 2),  # AIADMK
        160:(celeb_map.get("Seeman"), 11),           # NTK
        183:(celeb_map.get("Tamilisai Soundararajan"), 4),  # BJP
        175:(celeb_map.get("Sarathkumar"), 14),      # AISMK (mapped to DMDK slot)
    }
    dmk_locked = {186: 1, 182: 1}
    aiadmk_locked = {50: 2, 183: 4, 175: 14}
    dmk_map = assign_2026_alliance_seats(dc, DMK_2026_ALLOCATIONS, locked=dmk_locked)
    aiadmk_map = assign_2026_alliance_seats(dc, AIADMK_2026_ALLOCATIONS, locked=aiadmk_locked)

    used=set()
    records=[]
    used.update(all_celeb_ids)

    def next_candidate():
        avail=[c for c in all_cids if c not in used and c not in all_celeb_ids]
        if not avail:
            avail=[c for c in all_cids if c not in all_celeb_ids]
        cand_id=int(rng.choice(avail[:700] if len(avail)>700 else avail))
        used.add(cand_id)
        return cand_id

    for _,cr in dc.iterrows():
        cid=int(cr["constituency_id"])
        verified_rows = VERIFIED_2026_NOMINATIONS.get(cid)
        if verified_rows:
            for row in verified_rows:
                cand_id = int(candidate_lookup[row["candidate_name"]])
                used.add(cand_id)
                records.append({
                    "constituency_id":cid,
                    "candidate_id":cand_id,
                    "party_id":int(row["party_id"]),
                    "nomination_status":"Filed",
                    "scrutiny_status":"Accepted",
                    "withdrawal_status":"Not Withdrawn",
                    "final_contesting_flag":bool(row["final_contesting_flag"]),
                    "affidavit_url":None,
                    "source_date":"2026-04-01"
                })
            continue
        structured_parties = [dmk_map[cid], aiadmk_map[cid], 11, 17]
        extras = [15] if rng.random() < 0.35 else []

        for pid in structured_parties + extras:
            nom="Filed"
            scr="Accepted"
            wd="Not Withdrawn"
            final=True

            forced = celeb_2026.get(cid)
            if forced and forced[1] == pid:
                cand_id = forced[0]
                used.discard(cand_id)
                used.add(cand_id)
            else:
                cand_id = next_candidate()

            if pid == 15:
                scr = rng.choice(["Accepted","Accepted","Rejected"])
                wd = rng.choice(["Not Withdrawn","Withdrawn"]) if scr == "Accepted" else None
                final = scr == "Accepted" and wd == "Not Withdrawn"

            records.append({
                "constituency_id":cid,
                "candidate_id":cand_id,
                "party_id":int(pid),
                "nomination_status":nom,
                "scrutiny_status":scr,
                "withdrawal_status":wd,
                "final_contesting_flag":final,
                "affidavit_url":None,
                "source_date":"2026-04-01"
            })
    df=pd.DataFrame(records)
    df.to_parquet(PARQUET_DIR/"fact_nominations_2026.parquet",index=False)
    print(f"[ok] fact_nominations_2026: {len(df)} rows"); return df


def build_dim_constituency_scd(dc):
    election_years = [2001, 2006, 2011, 2016, 2021, 2026]
    records = []
    sk = 1
    for idx, year in enumerate(election_years):
        next_year = election_years[idx + 1] if idx + 1 < len(election_years) else None
        valid_to = next_year - 1 if next_year else 9999
        for _, row in dc.iterrows():
            records.append({
                "constituency_sk": sk,
                "constituency_id": int(row["constituency_id"]),
                "constituency_name": row["constituency_name"],
                "district": row["district"],
                "type": row["type"],
                "region": row["region"],
                "valid_from_year": year,
                "valid_to_year": valid_to,
                "election_year": year,
                "is_current": year == 2026,
            })
            sk += 1
    df = pd.DataFrame(records)
    df.to_parquet(PARQUET_DIR / "dim_constituency_scd.parquet", index=False)
    print(f"[ok] dim_constituency_scd: {len(df)} rows"); return df


def _historical_bloc_name(party_id):
    if party_id in [1,3,5,6,7,8,9,13]:
        return "DMK Alliance"
    if party_id in [2,10,14,16]:
        return "AIADMK Alliance"
    if party_id == 11:
        return "NTK"
    if party_id == 17:
        return "TVK"
    return "Others"


def _forecast_band(prob):
    if prob >= 0.8:
        return "Safe"
    if prob >= 0.6:
        return "Lean"
    if prob >= 0.4:
        return "Toss-up"
    if prob >= 0.2:
        return "Lean"
    return "Safe"


def build_fact_constituency_contest(dcscd, fr, fn, sw):
    sk_map = {
        (int(row["constituency_id"]), int(row["election_year"])): int(row["constituency_sk"])
        for _, row in dcscd.iterrows()
    }
    records = []

    for _, row in fr.iterrows():
        party_id = int(row["party_id"])
        votes = int(row["votes"]) if pd.notna(row["votes"]) else None
        vote_share = float(row["vote_share"]) if pd.notna(row["vote_share"]) else None
        rank = int(row["rank"]) if pd.notna(row["rank"]) else None
        records.append({
            "election_year": int(row["election_year"]),
            "constituency_sk": sk_map[(int(row["constituency_id"]), int(row["election_year"]))],
            "constituency_id": int(row["constituency_id"]),
            "candidate_id": int(row["candidate_id"]),
            "party_id": party_id,
            "bloc_name": _historical_bloc_name(party_id),
            "record_type": "historical_result",
            "contesting_flag": True,
            "winner_flag": bool(row["winner_flag"]),
            "rank": rank,
            "votes": votes,
            "vote_share": vote_share,
            "margin_votes": int(row["margin_votes"]) if pd.notna(row["margin_votes"]) else None,
            "turnout_pct": float(row["turnout_pct"]) if pd.notna(row["turnout_pct"]) else None,
            "forecast_win_prob": None,
            "forecast_band": None,
            "source_type": "modeled_historical",
        })

    swing_map = {
        int(row["constituency_id"]): row
        for _, row in sw.iterrows()
    }
    bloc_lookup = {
        "DMK Alliance": lambda r: float(r["dmk_strength_score"]),
        "AIADMK Alliance": lambda r: float(r["aiadmk_strength_score"]),
        # NTK and TVK are modeled as statewide spoiler / entrant forces, not
        # co-equal statewide favorites to the two major alliances.
        "NTK": lambda r: min(24.0, 8.0 + float(r["ntk_vote_split_pct"]) * 0.45),
        "TVK": lambda r: min(28.0, 10.0 + float(r["tvk_factor_pct"]) * 0.75),
    }

    final_fn = fn[fn["final_contesting_flag"] == True].copy()
    for _, row in final_fn.iterrows():
        party_id = int(row["party_id"])
        if party_id in DMK_2026_PARTIES:
            bloc_name = "DMK Alliance"
        elif party_id in AIADMK_2026_PARTIES:
            bloc_name = "AIADMK Alliance"
        elif party_id == 11:
            bloc_name = "NTK"
        elif party_id == 17:
            bloc_name = "TVK"
        else:
            bloc_name = "Others"

        swing_row = swing_map[int(row["constituency_id"])]
        raw_scores = {
            key: value(swing_row)
            for key, value in bloc_lookup.items()
        }
        exp_scores = {key: np.exp(score / 14.0) for key, score in raw_scores.items()}
        total_score = sum(exp_scores.values())
        win_prob = exp_scores.get(bloc_name, 0.0) / total_score if total_score else 0.0

        records.append({
            "election_year": 2026,
            "constituency_sk": sk_map[(int(row["constituency_id"]), 2026)],
            "constituency_id": int(row["constituency_id"]),
            "candidate_id": int(row["candidate_id"]),
            "party_id": party_id,
            "bloc_name": bloc_name,
            "record_type": "forecast_2026",
            "contesting_flag": True,
            "winner_flag": False,
            "rank": None,
            "votes": None,
            "vote_share": None,
            "margin_votes": None,
            "turnout_pct": None,
            "forecast_win_prob": round(float(win_prob), 4),
            "forecast_band": _forecast_band(float(win_prob)),
            "source_type": "modeled_forecast",
        })

    df = pd.DataFrame(records)
    df.to_parquet(PARQUET_DIR / "fact_constituency_contest.parquet", index=False)
    print(f"[ok] fact_constituency_contest: {len(df)} rows"); return df

def build_fact_voter_demographics(dc):
    records=[]
    for year in [2001,2006,2011,2016,2021,2026]:
        for _,cr in dc.iterrows():
            cid=int(cr["constituency_id"]); region=cr["region"]
            base=int(rng.integers(130000,220000))
            growth=1+0.055*(year-2001)/5; total=int(base*growth)
            yb=1.25 if region=="Chennai" else (1.1 if region=="West" else 1.0)
            yd=max(0.8,1-0.03*(year-2001)/5)
            a18=rng.uniform(0.03,0.045)*yb*yd; a29=rng.uniform(0.17,0.23)*yb*yd
            a39=rng.uniform(0.19,0.24); a49=rng.uniform(0.17,0.22)
            a59=rng.uniform(0.12,0.16); a60=max(0.05,1-a18-a29-a39-a49-a59)
            mp=rng.uniform(0.49,0.52); male=int(total*mp); third=int(rng.integers(5,60))
            tr_range=KNOWN_TURNOUT.get(year,(68,78))
            turnout=float(rng.uniform(tr_range[0]/100,tr_range[1]/100)) if year<=2021 else None
            records.append({"election_year":year,"constituency_id":cid,"total_electors":total,"male_electors":male,"female_electors":int(total-male-third),"third_gender_electors":int(third),"age_18_19":int(total*a18),"age_20_29":int(total*a29),"age_30_39":int(total*a39),"age_40_49":int(total*a49),"age_50_59":int(total*a59),"age_60_plus":int(total*a60),"turnout_pct":round(turnout*100,2) if turnout else None})
    df=pd.DataFrame(records)
    df.to_parquet(PARQUET_DIR/"fact_voter_demographics.parquet",index=False)
    print(f"[ok] fact_voter_demographics: {len(df)} rows (6 years)"); return df

def build_dim_celebrity_constituency():
    rows=[{"constituency_id":s[0],"election_year":s[1],"celebrity_flag":s[2],"celebrity_reason":s[3],"celebrity_candidate_name":s[4],"celebrity_type":s[5]} for s in CELEB_SEATS]
    df=pd.DataFrame(rows)
    df.to_parquet(PARQUET_DIR/"dim_celebrity_constituency.parquet",index=False)
    print(f"[ok] dim_celebrity_constituency: {len(df)} rows"); return df

def build_fact_2026_swing(dc):
    rng2=np.random.default_rng(77); records=[]
    for _,cr in dc.iterrows():
        cid=int(cr["constituency_id"]); region=cr["region"]; district=cr["district"]
        dmk=float(rng2.uniform(40,85)) if region in ["Chennai","Delta","Central"] else float(rng2.uniform(28,68))
        aiadmk=max(15,min(85,100-dmk+float(rng2.uniform(-15,15))))
        ntk=float(rng2.uniform(5,22)); bjp=float(rng2.uniform(2,12)) if district in ["Chennai","Coimbatore","Vellore"] else float(rng2.uniform(1,6))
        tvk=float(rng2.uniform(3,18)); swing=abs(dmk-aiadmk)<15
        winner="DMK Alliance" if dmk>aiadmk else "AIADMK"
        conf=round(abs(dmk-aiadmk)/50,2)
        records.append({"constituency_id":cid,"dmk_strength_score":round(dmk,1),"aiadmk_strength_score":round(aiadmk,1),"ntk_vote_split_pct":round(ntk,1),"bjp_factor_pct":round(bjp,1),"tvk_factor_pct":round(tvk,1),"swing_seat_flag":bool(swing),"expected_winner_2026":winner,"confidence_score":conf,"analysis_note":f"{'Swing seat' if swing else 'Leaning '+winner}. NTK may draw ~{round(ntk,1)}% of votes. TVK (new): {round(tvk,1)}%. BJP base: {round(bjp,1)}%."})
    df=pd.DataFrame(records)
    df.to_parquet(PARQUET_DIR/"fact_2026_swing.parquet",index=False)
    print(f"[ok] fact_2026_swing: {len(df)} rows"); return df

def write_modeled_source_metadata():
    metadata = {
        "mode": "modeled",
        "description": (
            "Modeled Tamil Nadu election warehouse generated locally for forecast and UX testing. "
            "Historical results are synthetic/model-calibrated, and 2026 rows represent contest/forecast data rather than certified outcomes."
        ),
        "generated_on": "2026-04-17",
        "generator": "generate_data.py",
    }
    SOURCE_METADATA_PATH.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    print("[ok] source_metadata.json written for modeled dataset")

if __name__=="__main__":
    print("Generating Tamil Nadu Elections Data v2 (2001-2026)...")
    dc=build_dim_constituency(); dp=build_dim_party(); dcand=build_dim_candidate()
    fr=build_fact_results(dc,dcand); fn=build_fact_nominations_2026(dc,dcand)
    fv=build_fact_voter_demographics(dc); dcc=build_dim_celebrity_constituency()
    sw=build_fact_2026_swing(dc)
    dcscd=build_dim_constituency_scd(dc)
    fc=build_fact_constituency_contest(dcscd, fr, fn, sw)
    write_modeled_source_metadata()
    print("\nDone. Elections: 2001, 2006, 2011, 2016, 2021, 2026")
