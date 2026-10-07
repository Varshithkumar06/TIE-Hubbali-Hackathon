"""
AMARGOL LOAD HELP — INBOUND EXOTEL AGENTSTREAM BACKEND
=======================================================
Backend-only voice agent for the inbound Exotel Voicebot / AgentStream flow.

Call path
  Caller -> ExoPhone 080-472-89156 -> Exotel Voicebot -> WSS /exotel/ws
  -> Sarvam STT -> dialogue engine -> Sarvam TTS -> WSS -> caller

This build intentionally has no browser frontend, no dashboard WebSocket, no
browser simulator, no Twilio handling, and no outbound Exotel REST dialing.
It only exposes the backend connections needed by the inbound Voicebot flow.

Required runtime settings
  SARVAM_API_KEY=...
  PUBLIC_URL=https://<public-domain>
  EXOTEL_STREAM_URL=wss://<public-domain>/exotel/ws
  EXOTEL_CALLER_ID=080-472-89156
  TELEPHONY=exotel

Exotel Voicebot Applet
  Static URL:
      wss://<public-domain>/exotel/ws
  or Dynamic URL:
      https://<public-domain>/exotel/resolve

Health / configuration endpoints
  GET /healthz
  GET /exotel/config
  GET /exotel/resolve

The backend receives Exotel connected/start/media/dtmf/stop events and sends
bidirectional media/mark events. Exotel's Voicebot AgentStream uses raw signed
16-bit little-endian mono PCM, normally 8 kHz, transported as base64 JSON media.
"""
import base64
import io
import json
import os
import re
import sqlite3
import statistics
import math
import struct
import socket
import sys
import threading
import time
import uuid
import wave
import urllib.parse
from datetime import datetime, timedelta, timezone
from xml.sax.saxutils import escape as xesc

try:
    from sarvamai import SarvamAI
except ImportError:                              
    SarvamAI = None

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

                                                                       
E = os.environ.get
SARVAM_KEY = E("SARVAM_API_KEY", "")
PORT = int(E("PORT", "8000"))
HOST = E("HOST", "127.0.0.1")
PUBLIC_URL = E("PUBLIC_URL", "").rstrip("/")
if not PUBLIC_URL and E("VERCEL_URL", ""):
    PUBLIC_URL = "https://" + E("VERCEL_URL", "").rstrip("/")
DB_PATH = E("DB_PATH", "amargol.db")
DEFAULT_LANG = E("DEFAULT_LANG", "kn")
STT_MODEL = E("SARVAM_STT_MODEL", "saarika:v2.5")
TTS_MODEL = E("SARVAM_TTS_MODEL", "bulbul:v3")
VOICE = E("VOICE", "shubh")
PROVIDER = E("TELEPHONY", "exotel").lower()                                  
EXO_CALLER = E("EXOTEL_CALLER_ID", "080-472-89156")
EXO_EXOPHONE = E("EXOTEL_NUMBER", EXO_CALLER or "080-472-89156")
EXOTEL_MODE = E("EXOTEL_MODE", "inbound_voicebot")
_DEFAULT_EXO_STREAM = ((PUBLIC_URL.replace("https://", "wss://", 1).replace("http://", "ws://", 1) + "/exotel/ws") if PUBLIC_URL else "")
EXO_STREAM_URL = E("EXOTEL_STREAM_URL", _DEFAULT_EXO_STREAM)
ALLOW_SELF_MATCH = E("ALLOW_SELF_MATCH", "1") == "1"

HUB = "Amargol"
TRUCK_CAPACITY_T = 15
OFFER_TTL = 600                                                        
MAX_ATTEMPTS = 3
IST = timezone(timedelta(hours=5, minutes=30))
LANGS = ["en", "hi", "kn", "ta", "te", "ml", "mr", "gu", "bn", "pa", "or", "as"]
if DEFAULT_LANG not in LANGS:
    DEFAULT_LANG = "kn"
LANG_CODE = {
    "en": "en-IN",
    "hi": "hi-IN",
    "kn": "kn-IN",
    "ta": "ta-IN",
    "te": "te-IN",
    "ml": "ml-IN",
    "mr": "mr-IN",
    "gu": "gu-IN",
    "bn": "bn-IN",
    "pa": "pa-IN",
    "or": "or-IN",
    "as": "as-IN",
}

client = SarvamAI(api_subscription_key=SARVAM_KEY) if (SarvamAI and SARVAM_KEY) else None


def log(msg):
    ex("INSERT INTO events(ts,msg) VALUES(?,?)", (time.time(), msg))
    print("[%s] %s" % (datetime.now(IST).strftime("%H:%M:%S"), msg))


def tlog(tag, msg):
    print("[%s] %-10s %s" % (datetime.now(IST).strftime("%H:%M:%S"), tag, msg), flush=True)


                                                                         
LOCK = threading.RLock()
SCHEMA_VERSION = 2
_db = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
_db.row_factory = sqlite3.Row
_db.execute("PRAGMA journal_mode=WAL")

                                                               
if _db.execute("PRAGMA user_version").fetchone()[0] != SCHEMA_VERSION:
    for (_t,) in _db.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall():
        _db.execute("DROP TABLE IF EXISTS " + _t)
    _db.execute("PRAGMA user_version=%d" % SCHEMA_VERSION)

_db.executescript("""
CREATE TABLE IF NOT EXISTS users(phone TEXT PRIMARY KEY, role TEXT, lang TEXT);
CREATE TABLE IF NOT EXISTS trucks(id INTEGER PRIMARY KEY AUTOINCREMENT, phone TEXT, destination TEXT,
  capacity_tons REAL, available_capacity_tons REAL, available_until REAL, expected_price REAL,
  status TEXT, created REAL);
CREATE TABLE IF NOT EXISTS loads(id INTEGER PRIMARY KEY AUTOINCREMENT, phone TEXT, destination TEXT,
  weight_tons REAL, required_by REAL, offered_price REAL, status TEXT, created REAL);
CREATE TABLE IF NOT EXISTS bookings(id INTEGER PRIMARY KEY AUTOINCREMENT, truck_id INTEGER, load_id INTEGER,
  destination TEXT, weight_tons REAL, price REAL, status TEXT, driver_ok INTEGER DEFAULT 0,
  trader_ok INTEGER DEFAULT 0, price_by TEXT, created REAL, updated REAL);
CREATE TABLE IF NOT EXISTS rejections(truck_id INTEGER, load_id INTEGER);
CREATE TABLE IF NOT EXISTS callbacks(id INTEGER PRIMARY KEY AUTOINCREMENT, phone TEXT, role TEXT, kind TEXT,
  booking_id INTEGER, msgkey TEXT, params TEXT, status TEXT, attempts INTEGER DEFAULT 0,
  next_try REAL, dialed_at REAL, created REAL);
CREATE TABLE IF NOT EXISTS rates(origin TEXT, dest TEXT, per_tonne REAL, PRIMARY KEY(origin,dest));
CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, msg TEXT);
""")
                                                                                         
for _d, _r in {"Mysuru": 2500, "Davanagere": 700, "Bengaluru": 1400, "Pune": 2200, "Hubballi": 450,
               "Belagavi": 700, "Mangaluru": 1800, "Hyderabad": 2000, "Shivamogga": 900, "Ballari": 1000}.items():
    _db.execute("INSERT OR IGNORE INTO rates VALUES(?,?,?)", (HUB, _d, _r))


def q(sql, args=()):
    with LOCK:
        return [dict(r) for r in _db.execute(sql, args).fetchall()]


def q1(sql, args=()):
    r = q(sql, args)
    return r[0] if r else None


def ex(sql, args=()):
    with LOCK:
        return _db.execute(sql, args).lastrowid


                                                                                                        
CITIES = {                                              
    "Amargol": ("ಅಮರಗೋಳ", "अमरगोल", ["amargol", "amaragol", "amargola"]),
    "Mysuru": ("ಮೈಸೂರು", "मैसूर", ["mysuru", "mysore", "maisuru", "ಮೈಸೂರ", "ಮೈಸೂರಿ"]),
    "Davanagere": ("ದಾವಣಗೆರೆ", "दावणगेरे", ["davanagere", "davangere", "davanagiri"]),
    "Bengaluru": ("ಬೆಂಗಳೂರು", "बेंगलुरु", ["bengaluru", "bangalore", "bengalooru", "ಬೆಂಗಳೂರ", "ಬೆಂಗಳೂರಿ", "बैंगलोर"]),
    "Hubballi": ("ಹುಬ್ಬಳ್ಳಿ", "हुबली", ["hubballi", "hubli"]),
    "Dharwad": ("ಧಾರವಾಡ", "धारवाड़", ["dharwad", "dharwar"]),
    "Belagavi": ("ಬೆಳಗಾವಿ", "बेलगावी", ["belagavi", "belgaum", "belgam", "बेलगाम"]),
    "Mangaluru": ("ಮಂಗಳೂರು", "मंगलुरु", ["mangaluru", "mangalore", "ಮಂಗಳೂರ", "ಮಂಗಳೂರಿ"]),
    "Shivamogga": ("ಶಿವಮೊಗ್ಗ", "शिवमोग्गा", ["shivamogga", "shimoga"]),
    "Ballari": ("ಬಳ್ಳಾರಿ", "बेल्लारी", ["ballari", "bellary"]),
    "Kalaburagi": ("ಕಲಬುರಗಿ", "कलबुर्गी", ["kalaburagi", "gulbarga"]),
    "Vijayapura": ("ವಿಜಯಪುರ", "विजयपुर", ["vijayapura", "bijapur"]),
    "Hyderabad": ("ಹೈದರಾಬಾದ್", "हैदराबाद", ["hyderabad"]),
    "Pune": ("ಪುಣೆ", "पुणे", ["pune", "poona"]),
    "Mumbai": ("ಮುಂಬೈ", "मुंबई", ["mumbai", "bombay"]),
    "Chennai": ("ಚೆನ್ನೈ", "चेन्नई", ["chennai", "madras"]),
}


def city_name(c, lang):
    kn, hi, _ = CITIES[c]
    return kn if lang == "kn" else hi if lang == "hi" else c


def has_any(t, words):
    for w in words:
        if w.isascii():
            if re.search(r"(?<![a-z])" + re.escape(w) + r"(?![a-z])", t):
                return True
        elif w in t:
            return True
    return False


def has_re(t, pats):
    return any(re.search(p, t) for p in pats)


def find_cities(t):
    hits = []
    for c, (kn, hi, al) in CITIES.items():
        pos = []
        for a in [kn, hi] + [c.lower()] + al:
            if a.isascii():
                m = re.search(r"(?<![a-z])" + re.escape(a.lower()) + r"(?![a-z])", t)
                if m:
                    pos.append(m.start())
            elif a in t:
                pos.append(t.index(a))
        if pos:
            hits.append((min(pos), c))
    return [c for _, c in sorted(hits)]


NUMW = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 9,
        "ten": 10, "twelve": 12, "fifteen": 15, "twenty": 20,
        "ek": 1, "do": 2, "teen": 3, "char": 4, "chaar": 4, "paanch": 5, "panch": 5, "chhe": 6, "che": 6,
        "saat": 7, "aath": 8, "nau": 9, "das": 10,
        "एक": 1, "दो": 2, "तीन": 3, "चार": 4, "पाँच": 5, "पांच": 5, "छह": 6, "छः": 6, "सात": 7, "आठ": 8,
        "नौ": 9, "दस": 10, "पंद्रह": 15, "बीस": 20,
        "ondu": 1, "eradu": 2, "mooru": 3, "muru": 3, "naalku": 4, "nalku": 4, "aidu": 5, "aaru": 6,
        "elu": 7, "entu": 8, "ombattu": 9, "hattu": 10,
        "ಒಂದು": 1, "ಎರಡು": 2, "ಮೂರು": 3, "ನಾಲ್ಕು": 4, "ಐದು": 5, "ಆರು": 6, "ಏಳು": 7, "ಎಂಟು": 8,
        "ಒಂಬತ್ತು": 9, "ಹತ್ತು": 10}
_asc = sorted([re.escape(w) for w in NUMW if w.isascii()], key=len, reverse=True)
_scr = sorted([re.escape(w) for w in NUMW if not w.isascii()], key=len, reverse=True)
NUM = r"(\d+(?:\.\d+)?|(?<![a-z])(?:%s)(?![a-z])|%s)" % ("|".join(_asc), "|".join(_scr))
THOU = r"(?:thousand|hazaar|hazar|hajar|saavira|savira|ಸಾವಿರ|हज़ार|हजार)"
UNIT_T = r"(?:\btonn?es?\b|\btons?\b|ಟನ್|टन)"
UNIT_H = r"(?:\bhours?\b|\bhrs?\b|\bghant\w*|\bgant\w*|ಗಂಟೆ|घंटे|घंटा|घण्टे)"
UNIT_M = r"(?:\bminutes?\b|\bmins?\b|ನಿಮಿಷ|मिनट)"
PRICE_U = r"(?:₹|\brs\b\.?|\brupees?\b|\brupaye\b|\binr\b|ರೂಪಾಯಿ|ರೂ|रुपये|रुपए|रुपया|रु)"
_DIG = str.maketrans("೦೧೨೩೪೫೬೭೮೯०१२३४५६७८९", "01234567890123456789")


def val(x):
    try:
        return float(x)
    except ValueError:
        return float(NUMW[x.lower()])


def norm(text):
    t = (text or "").lower().translate(_DIG).replace("ಇಲ್ಲಿ", " ")
    t = re.sub(r"(?<=\d),(?=\d{3})", "", t)
    t = re.sub(NUM + r"\s*" + THOU, lambda m: str(int(val(m.group(1)) * 1000)), t)
    return re.sub(r"\s+", " ", t).strip()


TOMORROW = ["tomorrow", "tmrw", "kal", "naale", "nale", "ನಾಳೆ", "कल"]
PERIODS = [(["morning", "subah", "सुबह", "ಬೆಳಿಗ್ಗೆ", "ಬೆಳಗ್ಗೆ"], 9), (["afternoon", "dopahar", "दोपहर", "ಮಧ್ಯಾಹ್ನ"], 14),
           (["evening", "shaam", "शाम", "ಸಂಜೆ"], 18), (["night", "raat", "रात", "ರಾತ್ರಿ"], 21)]
NOW_W = ["now", "immediately", "asap", "right away", "abhi", "turant", "ತಕ್ಷಣ", "ಈಗ", "अभी", "तुरंत"]
HALF_HOUR_W = ["half an hour", "half hour", "aadha ghanta", "aadhe ghante",
               "आधा घंटा", "आधा घंटे", "अर्धा घंटा", "ಅರ್ಧ ಗಂಟೆ"]


def parse_when(t, now):
    """Return an absolute epoch for a relative/absolute time expression."""
    d0 = datetime.fromtimestamp(now, IST)
    if has_any(t, HALF_HOUR_W):
        return now + 30 * 60
    tom = has_any(t, TOMORROW)
    period = next((h for ws, h in PERIODS if has_any(t, ws)), None)
    m = re.search(r"(\d{1,2})(?::(\d{2}))?\s*(am|pm|a\.m\.|p\.m\.|baje|बजे|ಗಂಟೆಗೆ)", t)
    hour = minute = None
    if m:
        hour, minute, suf = int(m.group(1)), int(m.group(2) or 0), m.group(3)
        if suf.startswith("p") and hour < 12:
            hour += 12
        elif suf.startswith("a") and hour == 12:
            hour = 0
        elif not suf[0] in "ap" and period and period >= 14 and hour < 12:
            hour += 12
    elif period is not None:
        return None
    elif tom:
        return None
    if hour is not None and 0 <= hour < 24 and 0 <= minute < 60:
        tgt = d0.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if tom:
            tgt += timedelta(days=1)
        elif tgt <= d0:
            tgt += timedelta(days=1)
        return tgt.timestamp()
    m = re.search(NUM + r"\s*" + UNIT_H, t)
    if m:
        return now + val(m.group(1)) * 3600
    m = re.search(NUM + r"\s*" + UNIT_M, t)
    if m:
        return now + val(m.group(1)) * 60
    if has_any(t, NOW_W):
        return now + 1800
    return None


def extract(t, now):
    cs = find_cities(t)
    dests = [c for c in cs if c != HUB] or cs
    m = re.search(NUM + r"\s*" + UNIT_T, t)
    tons = val(m.group(1)) if m else None
    m = re.search(PRICE_U + r"\s*" + NUM, t) or re.search(NUM + r"\s*" + PRICE_U, t)
    price = val(m.group(1)) if m else None
    return {"dest": dests[-1] if dests else None, "tons": tons, "ts": parse_when(t, now), "price": price}


YES = ["yes", "yeah", "yep", "ok", "okay", "sure", "confirm", "accept", "done", "haan", "han", "haa", "ha", "ji",
       "theek", "thik", "sahi", "ಹೌದು", "ಹಾಂ", "ಹಾ", "ಸರಿ", "ಓಕೆ", "ಒಪ್ಪ", "ಆಗಲಿ", "ಆಯ್ತು", "हाँ", "हां", "हा",
       "जी", "ठीक", "सही", "मंज़ूर", "ಯೆಸ್"]
NO = ["no", "nope", "nahi", "nahin", "na", "reject", "decline", "beda", "ಇಲ್ಲ", "ಬೇಡ", "ನೋ", "नहीं", "नही", "ना",
      "मत", "बेडा"]
BYE = ["bye", "thank you", "thanks", "nothing", "that's all", "bas", "dhanyavad", "shukriya", "ಧನ್ಯವಾದ", "ಸಾಕು",
       "धन्यवाद", "बस", "शुक्रिया"]
HUMAN = ["human", "real person", "operator", "helper", "ಮನುಷ್ಯ", "ಅಧಿಕಾರಿ", "इंसान", "ऑपरेटर", "ಹ್ಯೂಮನ್"]
DRIVER_W = ["driver", "my truck", "my lorry", "i'm going", "i am going", "capacity", "space", "empty", "return load",
            "ಡ್ರೈವರ್", "ಚಾಲಕ", "ನನ್ನ ಟ್ರಕ್", "ನನ್ನ ಲಾರಿ", "ಖಾಲಿ", "ड्राइवर", "ड्राईवर", "चालक", "मेरा ट्रक", "खाली",
            "ડ્રાઇવર", "ડ્રાઈવર", "મારી ટ્રક", "મારી લારી", "ખાલી", "ड्रायव्हर", "माझा ट्रक",
            "டிரைவர்", "என் டிரக்", "காலி", "డ్రైవర్", "నా ట్రక్", "ఖాళీ", "ഡ്രൈവർ", "എന്റെ ട്രക്ക്",
            "ড্ৰাইভাৰ", "মোৰ ট্ৰাক", "খালী", "ਡਰਾਈਵਰ", "ਮੇਰਾ ਟਰੱਕ", "ਖਾਲੀ"]
TRADER_W = ["trader", "farmer", "have a load", "have load", "my load", "need a truck", "need truck", "want a truck",
            "goods", "maal", "merchant", "ವ್ಯಾಪಾರಿ", "ರೈತ", "ನನ್ನ ಲೋಡ್", "ಲೋಡ್ ಇದೆ", "ಮಾಲು", "ಸರಕು", "ಟ್ರಕ್ ಬೇಕು",
            "ಲಾರಿ ಬೇಕು", "व्यापारी", "किसान", "माल", "लोड है", "ट्रक चाहिए", "ट्रक चाहिये", "गाड़ी चाहिए",
            "load છે", "લોડ છે", "મારી પાસે લોડ", "ટ્રક જોઈએ", "माल आहे", "माझ्याकडे माल", "ट्रक पाहिजे",
            "லோடு இருக்கு", "என்னிடம் லோடு", "ట్రక్ కావాలి", "లోడ్ ఉంది", "എന്റെ ലോഡ്", "ലോഡ് ഉണ്ട്",
            "আমার মাল", "মাল আছে", "ਲੋਡ ਹੈ", "ਮੇਰੇ ਕੋਲ ਲੋਡ", "ମୋର ଲୋଡ୍", "ଲୋଡ୍ ଅଛି", "মোৰ মাল"]
TRUCK_W = ["truck", "lorry", "gaadi", "ಟ್ರಕ್", "ಲಾರಿ", "ट्रक", "गाड़ी"]
LOAD_W = ["load", "maal", "ಲೋಡ್", "लोड", "माल"]
DELIVERED = [r"deliver(ed|y)", r"unloaded", r"reached", r"पहुंच", r"पहुँच", r"डिलीवर", r"ತಲುಪ", r"ಡೆಲಿವರಿ", r"ಇಳಿಸ"]
LOADED = [r"load(ing)?\s*(is\s*)?(done|completed?|finished|over)", r"\bloaded\b", r"loading ho", r"लोडिंग (हो|पूरी|ख़त्म|खत्म)",
          r"लोड हो", r"ಲೋಡಿಂಗ್ (ಮುಗಿ|ಆಯ್ತು|ಆಗಿದೆ)", r"ಲೋಡ್ ಆ(ಯ್ತು|ಗಿದೆ)", r"ಲೋಡ್ ಮುಗಿ"]
TRANSIT = [r"left (the )?(yard|mandi|market)", r"\bleft\b", r"on (the|my) way", r"\bstarted\b", r"निकल", r"रवाना",
           r"चल (दिया|पड़ा)", r"ಹೊರಟ"]
RESCHED = [r"re-?schedul", r"postpone", r"change (the )?(time|date)", r"(need|want) (it|the truck|a truck)?\s?(tomorrow|later|at \d)",
           r"(don'?t|do not) need.*(in|within) .*(hour|minute)", r"बदल", r"आगे बढ़ा", r"ಬದಲಾಯಿಸ", r"ಮುಂದೂಡ",
           r"ನಾಳೆ ಬೇಕು", r"कल चाहिए"]
CANCEL = [r"cancel", r"(don'?t|do not) need.*(anymore|any more)", r"no longer", r"ರದ್ದು", r"ಕ್ಯಾನ್ಸಲ್", r"कैंसल", r"रद्द",
          r"ಇನ್ನು ಬೇಡ", r"अब नहीं चाहिए", r"ab nahi chahiye"]
RATE = ["rate", "charges", "charge", "fare", "bhada", "bhaada", "kiraya", "kiraaya", "ದರ", "ಬಾಡಿಗೆ", "रेट", "भाड़ा", "भाडा",
        "किराया", "दर"]
CHECK = [r"\bany (load|truck)", r"is there (any|a)", r"do you have (a|any)", r"koi (load|truck|gaadi)",
         r"load (hai|mila|milega)", r"ಇದೆಯಾ", r"ಇದೆಯೇ", r"ಸಿಗುತ", r"है क्या", r"मिलेगा", r"मिलेगी", r"कोई (लोड|ट्रक|गाड़ी)"]
KN_ROM = ["beku", "illa", "ide", "naale", "ondu", "eradu", "hogtha", "hogthide", "maadi", "namaskara", "yaavaga"]
HI_ROM = ["haan", "nahi", "chahiye", "hai", "kitna", "bhai", "mera", "abhi", "namaste", "kya", "kal"]


def detect_lang(text):
    scripts = {
        "kn": r"[\u0C80-\u0CFF]",
        "hi": r"[\u0900-\u097F]",
        "ta": r"[\u0B80-\u0BFF]",
        "te": r"[\u0C00-\u0C7F]",
        "ml": r"[\u0D00-\u0D7F]",
        "gu": r"[\u0A80-\u0AFF]",
        "bn": r"[\u0980-\u09FF]",
        "pa": r"[\u0A00-\u0A7F]",
        "or": r"[\u0B00-\u0B7F]",
    }
    counts = {lang: len(re.findall(pattern, text or "")) for lang, pattern in scripts.items()}
    detected = max(counts, key=counts.get)
    if counts[detected] > 0:
        return detected

    low = (text or "").lower()
    roman_sets = {
        "kn": ["namaskara", "kannada", "nanna", "nanage", "sari"],
        "hi": ["namaste", "hindi", "mera", "mujhe", "gaadi"],
        "ta": ["vanakkam", "tamil", "enakku", "ungal", "nandri"],
        "te": ["namaskaram", "telugu", "naaku", "meeru", "dhanyavadalu"],
        "ml": ["malayalam", "enikku", "ningal", "nanni"],
        "mr": ["marathi", "majha", "mala", "dhanyavaad"],
        "gu": ["kem cho", "gujarati", "mane", "tame", "aabhar"],
        "bn": ["nomoskar", "bengali", "amar", "apni", "dhonnobad"],
        "pa": ["sat sri akal", "punjabi", "tusi", "dhanvaad"],
        "or": ["odia", "odiya", "mora", "mu", "dhanyabad"],
        "as": ["assamese", "mur", "moi", "dhonyobad"],
        "en": ["english", "my home", "home", "city", "where", "going", "load", "truck", "yes", "no", "please", "repeat"],
    }
    for lang, words in roman_sets.items():
        if has_any(low, words):
            return lang
    return None


def detect_role(t):
    if re.search(r"\bi have " + NUM + r"\s*" + UNIT_T + r"\s*(going|to|for)\b", t):
        return "trader"
    d = sum(has_any(t, [w]) for w in DRIVER_W)
    r = sum(has_any(t, [w]) for w in TRADER_W)
    if d > r:
        return "driver"
    if r > d:
        return "trader"
    if d == 0:
        if has_any(t, TRUCK_W):
            return "driver"
        if has_any(t, LOAD_W):
            return "trader"
    return None


                                                                                         
M = {
 "greet": ("Namaskara, Amargol Load Help. Are you a driver, or do you have a load?",
           "नमस्कार, अमरगोल लोड हेल्प। आप ड्राइवर हैं, या आपके पास लोड है?",
           "ನಮಸ್ಕಾರ, ಅಮರಗೋಳ ಲೋಡ್ ಹೆಲ್ಪ್. ನೀವು ಡ್ರೈವರ್ ಆಗಿದ್ದೀರಾ, ಅಥವಾ ನಿಮ್ಮ ಬಳಿ ಲೋಡ್ ಇದೆಯಾ?"),
 "ask_role": ("Please tell me: are you a driver, or do you have a load?",
              "कृपया बताइए: आप ड्राइवर हैं, या आपके पास लोड है?",
              "ದಯವಿಟ್ಟು ಹೇಳಿ: ನೀವು ಡ್ರೈವರ್ ಆಗಿದ್ದೀರಾ, ಅಥವಾ ನಿಮ್ಮ ಬಳಿ ಲೋಡ್ ಇದೆಯಾ?"),
 "got_it": ("Thanks, got it. ", "धन्यवाद, समझ गया। ", "ಧನ್ಯವಾದಗಳು, ಅರ್ಥವಾಯಿತು. "),
 "dest_unrecognized": ("I didn't recognize that place. Could you please repeat the destination? ",
                      "मुझे वह जगह समझ नहीं आई। कृपया गंतव्य एक बार फिर बताइए। ",
                      "ಆ ಸ್ಥಳ ನನಗೆ ಗುರುತಾಗಲಿಲ್ಲ. ದಯವಿಟ್ಟು ಗಮ್ಯಸ್ಥಾನವನ್ನು ಮತ್ತೊಮ್ಮೆ ಹೇಳಿ. "),
 "dest_unavailable": ("Sorry, I can't deliver to that location right now. Here are the destinations where trucks are currently available. Please tell me the number. ",
                     "माफ़ कीजिए, उस जगह के लिए अभी डिलीवरी उपलब्ध नहीं है। ये वे गंतव्य हैं जहाँ अभी ट्रक उपलब्ध हैं। कृपया नंबर बताइए। ",
                     "ಕ್ಷಮಿಸಿ, ಆ ಸ್ಥಳಕ್ಕೆ ಈಗ ಡೆಲಿವರಿ ಲಭ್ಯವಿಲ್ಲ. ಈಗ ಟ್ರಕ್‌ಗಳು ಲಭ್ಯವಿರುವ ಗಮ್ಯಸ್ಥಾನಗಳು ಇವು. ದಯವಿಟ್ಟು ಸಂಖ್ಯೆಯನ್ನು ಹೇಳಿ. "),
 "dest_choose": ("Please tell me one number from the list. ",
                 "कृपया सूची में से एक नंबर बताइए। ",
                 "ದಯವಿಟ್ಟು ಪಟ್ಟಿಯಲ್ಲಿರುವ ಒಂದು ಸಂಖ್ಯೆಯನ್ನು ಮಾತ್ರ ಹೇಳಿ. "),
 "dest_bad_number": ("That number is not available. Please tell me one number from the list. ",
                    "वह नंबर उपलब्ध नहीं है। कृपया सूची में से एक नंबर बताइए। ",
                    "ಆ ಸಂಖ್ಯೆ ಲಭ್ಯವಿಲ್ಲ. ದಯವಿಟ್ಟು ಪಟ್ಟಿಯಲ್ಲಿರುವ ಒಂದು ಸಂಖ್ಯೆಯನ್ನು ಹೇಳಿ. "),
 "dest_none": ("I don't have any currently available truck destinations right now. Could you please tell me another destination? ",
                "अभी कोई उपलब्ध ट्रक गंतव्य नहीं है। कृपया कोई दूसरा गंतव्य बताइए। ",
                "ಈಗ ಯಾವುದೇ ಲಭ್ಯ ಟ್ರಕ್ ಗಮ್ಯಸ್ಥಾನಗಳಿಲ್ಲ. ದಯವಿಟ್ಟು ಬೇರೆ ಗಮ್ಯಸ್ಥಾನವನ್ನು ಹೇಳಿ. "),
 "q_dest_truck": ("Where is your truck going?", "आपका ट्रक कहाँ जा रहा है?", "ನಿಮ್ಮ ಟ್ರಕ್ ಎಲ್ಲಿಗೆ ಹೋಗುತ್ತಿದೆ?"),
 "q_cap": ("How much space is available in your truck?", "आपके ट्रक में कितनी जगह खाली है?",
           "ನಿಮ್ಮ ಟ್ರಕ್‌ನಲ್ಲಿ ಎಷ್ಟು ಜಾಗ ಖಾಲಿ ಇದೆ?"),
 "q_hours": ("How long can you wait for a load?", "आप लोड के लिए कितनी देर रुक सकते हैं?",
             "ಲೋಡ್‌ಗಾಗಿ ಎಷ್ಟು ಹೊತ್ತು ಕಾಯಬಹುದು?"),
 "q_price": ("What price do you expect for this trip?", "इस ट्रिप के लिए आप कितना किराया चाहते हैं?",
             "ಈ ಟ್ರಿಪ್‌ಗೆ ಎಷ್ಟು ಬಾಡಿಗೆ ಬೇಕು?"),
 "q_dest_load": ("Where does the load need to go?", "लोड कहाँ भेजना है?", "ಲೋಡ್ ಎಲ್ಲಿಗೆ ಹೋಗಬೇಕು?"),
 "q_weight": ("How much does the load weigh?", "लोड का वजन कितना है?", "ಲೋಡ್ ತೂಕ ಎಷ್ಟು?"),
 "q_when": ("When do you need the truck?", "ट्रक कब चाहिए?", "ಟ್ರಕ್ ಯಾವಾಗ ಬೇಕು?"),
 "q_when_specific": ("What specific time do you need the truck?", "आपको ट्रक किस समय चाहिए?", "ನಿಮಗೆ ಟ್ರಕ್ ಯಾವ ಸಮಯಕ್ಕೆ ಬೇಕು?"),
 "sum_truck": ("Got it. Your truck has {cap} tonnes available, is going to {dest}, can wait {hours}, and your expected price is {price} rupees.",
               "ठीक है। आपके ट्रक में {cap} टन जगह है, ट्रक {dest} जा रहा है, आप {hours} रुक सकते हैं, और आपका किराया {price} रुपये है।",
               "ಸರಿ. ನಿಮ್ಮ ಟ್ರಕ್‌ನಲ್ಲಿ {cap} ಟನ್ ಜಾಗ ಇದೆ, {dest} ಕಡೆಗೆ ಹೋಗುತ್ತಿದೆ, {hours} ಕಾಯಬಹುದು, ನಿಮ್ಮ ಬಾಡಿಗೆ {price} ರೂಪಾಯಿ."),
 "sum_load": ("Got it. You have {w} tonnes going to {dest} and need a truck {when}.",
              "ठीक है। आपका {w} टन माल {dest} जाना है और ट्रक {when} चाहिए।",
              "ಸರಿ. ನಿಮ್ಮ {w} ಟನ್ ಲೋಡ್ {dest} ಕಡೆಗೆ ಹೋಗಬೇಕು, ಟ್ರಕ್ {when} ಬೇಕು."),
 "no_match_truck": ("I couldn't find a load going to {dest} right now. I will keep your truck available and call you back as soon as a suitable load comes in.",
                    "अभी {dest} जाने वाला कोई लोड नहीं मिला। मैं आपका ट्रक उपलब्ध रखूँगा और लोड मिलते ही आपको फ़ोन करूँगा।",
                    "ಈಗ {dest} ಕಡೆಗೆ ಹೋಗುವ ಲೋಡ್ ಸಿಗಲಿಲ್ಲ. ನಿಮ್ಮ ಟ್ರಕ್ ಲಭ್ಯವಾಗಿರಿಸುತ್ತೇನೆ, ಸೂಕ್ತ ಲೋಡ್ ಬಂದ ತಕ್ಷಣ ನಿಮಗೆ ಕರೆ ಮಾಡುತ್ತೇನೆ."),
 "no_match_load": ("I couldn't find a truck with enough capacity going to {dest} right now. Your load stays active and I will call you back when a truck becomes available.",
                   "अभी {dest} जाने वाला पर्याप्त जगह वाला ट्रक नहीं मिला। आपका लोड सक्रिय रहेगा और ट्रक मिलते ही मैं आपको फ़ोन करूँगा।",
                   "ಈಗ {dest} ಕಡೆಗೆ ಹೋಗುವ ಸಾಕಷ್ಟು ಜಾಗವಿರುವ ಟ್ರಕ್ ಸಿಗಲಿಲ್ಲ. ನಿಮ್ಮ ಲೋಡ್ ಸಕ್ರಿಯವಾಗಿರುತ್ತದೆ, ಟ್ರಕ್ ಸಿಕ್ಕ ತಕ್ಷಣ ನಿಮಗೆ ಕರೆ ಮಾಡುತ್ತೇನೆ."),
 "offer_driver": ("I found a {w} tonne load going to {dest}. The trader needs the truck {when}. The price is {price} rupees. Would you like to accept this load?",
                  "मुझे {dest} जाने वाला {w} टन का लोड मिला है। व्यापारी को ट्रक {when} चाहिए। किराया {price} रुपये है। क्या आप यह लोड लेंगे?",
                  "{dest} ಕಡೆಗೆ ಹೋಗುವ {w} ಟನ್ ಲೋಡ್ ಸಿಕ್ಕಿದೆ. ವ್ಯಾಪಾರಿಗೆ ಟ್ರಕ್ {when} ಬೇಕು. ಬಾಡಿಗೆ {price} ರೂಪಾಯಿ. ಈ ಲೋಡ್ ಒಪ್ಪುತ್ತೀರಾ?"),
 "offer_driver_partial": ("I found a {total} tonne load going to {dest}. Your truck has {cap} tonnes empty, so you can take {w} tonnes. The price for {w} tonnes is {price} rupees. Do you want to take it?",
                          "मुझे {dest} जाने वाला {total} टन का लोड मिला है। आपके ट्रक में {cap} टन जगह खाली है, इसलिए आप {w} टन ले सकते हैं। {w} टन का किराया {price} रुपये है। क्या आप इसे लेंगे?",
                          "{dest} ಕಡೆಗೆ ಹೋಗುವ {total} ಟನ್ ಲೋಡ್ ಸಿಕ್ಕಿದೆ. ನಿಮ್ಮ ಟ್ರಕ್‌ನಲ್ಲಿ {cap} ಟನ್ ಖಾಲಿ ಜಾಗ ಇದೆ, ಆದ್ದರಿಂದ ನೀವು {w} ಟನ್ ತೆಗೆದುಕೊಳ್ಳಬಹುದು. {w} ಟನ್‌ಗೆ {price} ರೂಪಾಯಿ. ಇದನ್ನು ತೆಗೆದುಕೊಳ್ಳುತ್ತೀರಾ?"),
 "offer_trader": ("I found a truck with {cap} tonnes available going to {dest}. The driver's price is {price} rupees. Would you like to book it?",
                  "मुझे {dest} जाने वाला ट्रक मिला है जिसमें {cap} टन जगह है। ड्राइवर का किराया {price} रुपये है। क्या आप इसे बुक करेंगे?",
                  "{dest} ಕಡೆಗೆ ಹೋಗುವ, {cap} ಟನ್ ಜಾಗವಿರುವ ಟ್ರಕ್ ಸಿಕ್ಕಿದೆ. ಡ್ರೈವರ್ ಬಾಡಿಗೆ {price} ರೂಪಾಯಿ. ಇದನ್ನು ಬುಕ್ ಮಾಡುತ್ತೀರಾ?"),
 "offer_trader_partial": ("I found a truck with {cap} tonnes empty for your {total} tonne load to {dest}. The truck can take {w} tonnes now for {price} rupees. Do you want to book this partial load?",
                          "आपके {total} टन के {dest} वाले लोड के लिए मुझे {cap} टन खाली जगह वाला ट्रक मिला है। यह ट्रक अभी {w} टन ले सकता है, किराया {price} रुपये है। क्या आप यह हिस्सा बुक करना चाहते हैं?",
                          "ನಿಮ್ಮ {total} ಟನ್ {dest} ಲೋಡ್‌ಗೆ {cap} ಟನ್ ಖಾಲಿ ಜಾಗವಿರುವ ಟ್ರಕ್ ಸಿಕ್ಕಿದೆ. ಈ ಟ್ರಕ್ ಈಗ {w} ಟನ್ ತೆಗೆದುಕೊಳ್ಳಬಹುದು, ಬಾಡಿಗೆ {price} ರೂಪಾಯಿ. ಈ ಭಾಗವನ್ನು ಬುಕ್ ಮಾಡುತ್ತೀರಾ?"),
 "counter_pre": ("The other party has asked for {price} rupees instead. ",
                 "दूसरे पक्ष ने {price} रुपये की मांग की है। ",
                 "ಇನ್ನೊಂದು ಕಡೆಯವರು {price} ರೂಪಾಯಿ ಕೇಳಿದ್ದಾರೆ. "),
 "confirmed_driver": ("Confirmed. Your truck is matched with the {dest} load for {price} rupees. The trader will contact you for pickup details. You don't have to return empty. Drive safely.",
                      "पक्का हो गया। आपका ट्रक {dest} के लोड से जुड़ गया है, किराया {price} रुपये। व्यापारी पिकअप की जानकारी के लिए आपसे संपर्क करेगा। अब आपको खाली नहीं लौटना पड़ेगा। सुरक्षित चलाइए।",
                      "ಖಚಿತವಾಯಿತು. ನಿಮ್ಮ ಟ್ರಕ್ ಅನ್ನು {dest} ಲೋಡ್‌ಗೆ ಜೋಡಿಸಲಾಗಿದೆ, ಬಾಡಿಗೆ {price} ರೂಪಾಯಿ. ಪಿಕಪ್ ವಿವರಗಳಿಗಾಗಿ ವ್ಯಾಪಾರಿ ನಿಮ್ಮನ್ನು ಸಂಪರ್ಕಿಸುತ್ತಾರೆ. ಖಾಲಿ ಮರಳಬೇಕಾಗಿಲ್ಲ. ಸುರಕ್ಷಿತವಾಗಿ ಚಲಾಯಿಸಿ."),
 "confirmed_trader": ("Confirmed. The truck will take your {w} tonne load to {dest} for {price} rupees. The driver will contact you for pickup details.",
                      "पक्का हो गया। ट्रक आपका {w} टन का लोड {dest} ले जाएगा, किराया {price} रुपये। पिकअप की जानकारी के लिए ड्राइवर आपसे संपर्क करेगा।",
                      "ಖಚಿತವಾಯಿತು. ಟ್ರಕ್ ನಿಮ್ಮ {w} ಟನ್ ಲೋಡ್ ಅನ್ನು {dest} ಕಡೆಗೆ {price} ರೂಪಾಯಿಗೆ ಕೊಂಡೊಯ್ಯುತ್ತದೆ. ಪಿಕಪ್ ವಿವರಗಳಿಗಾಗಿ ಡ್ರೈವರ್ ನಿಮ್ಮನ್ನು ಸಂಪರ್ಕಿಸುತ್ತಾರೆ."),
 "accepted_wait": ("Okay. I have noted your yes. I will now check with the other party and call you back with the result.",
                   "ठीक है। आपकी हाँ दर्ज कर ली है। अब मैं दूसरे पक्ष से पूछता हूँ और नतीजे के साथ आपको फ़ोन करूँगा।",
                   "ಸರಿ. ನಿಮ್ಮ ಒಪ್ಪಿಗೆ ದಾಖಲಿಸಿದ್ದೇನೆ. ಈಗ ಇನ್ನೊಂದು ಕಡೆಯವರನ್ನು ಕೇಳಿ, ಫಲಿತಾಂಶದೊಂದಿಗೆ ನಿಮಗೆ ಕರೆ ಮಾಡುತ್ತೇನೆ."),
 "counter_sent": ("Okay. I have sent your price of {price} rupees to the other party and will call you back with their answer.",
                  "ठीक है। आपका {price} रुपये का भाव दूसरे पक्ष को भेज दिया है, उनके जवाब के साथ आपको फ़ोन करूँगा।",
                  "ಸರಿ. ನಿಮ್ಮ {price} ರೂಪಾಯಿ ಬೆಲೆಯನ್ನು ಇನ್ನೊಂದು ಕಡೆಯವರಿಗೆ ಕಳುಹಿಸಿದ್ದೇನೆ, ಅವರ ಉತ್ತರದೊಂದಿಗೆ ನಿಮಗೆ ಕರೆ ಮಾಡುತ್ತೇನೆ."),
 "rejected_keep": ("Okay, I have rejected this offer. Your request stays active and I will look for another match.",
                   "ठीक है, यह ऑफ़र रद्द कर दिया। आपकी रिक्वेस्ट सक्रिय है और मैं दूसरा मैच ढूँढूँगा।",
                   "ಸರಿ, ಈ ಆಫರ್ ತಿರಸ್ಕರಿಸಿದ್ದೇನೆ. ನಿಮ್ಮ ಮನವಿ ಸಕ್ರಿಯವಾಗಿದೆ, ಬೇರೆ ಹೊಂದಾಣಿಕೆ ಹುಡುಕುತ್ತೇನೆ."),
 "offer_gone": ("Sorry, that offer is no longer available. Your request stays active.",
                "क्षमा कीजिए, वह ऑफ़र अब उपलब्ध नहीं है। आपकी रिक्वेस्ट सक्रिय है।",
                "ಕ್ಷಮಿಸಿ, ಆ ಆಫರ್ ಈಗ ಲಭ್ಯವಿಲ್ಲ. ನಿಮ್ಮ ಮನವಿ ಸಕ್ರಿಯವಾಗಿದೆ."),
 "none_driver": ("I don't have an active truck for you. ", "आपका कोई सक्रिय ट्रक दर्ज नहीं है। ", "ನಿಮ್ಮ ಯಾವುದೇ ಸಕ್ರಿಯ ಟ್ರಕ್ ದಾಖಲಾಗಿಲ್ಲ. "),
 "none_trader": ("I don't have an active load for you. ", "आपका कोई सक्रिय लोड दर्ज नहीं है। ", "ನಿಮ್ಮ ಯಾವುದೇ ಸಕ್ರಿಯ ಲೋಡ್ ದಾಖಲಾಗಿಲ್ಲ. "),
 "what_booking": ("booking", "बुकिंग", "ಬುಕಿಂಗ್"), "what_load": ("load", "लोड", "ಲೋಡ್"),
 "what_truck": ("truck", "ट्रक", "ಟ್ರಕ್"),
 "cancel_ask": ("I found your {dest} {what} for {w} tonnes. Do you want to cancel it?",
                "मुझे आपकी {dest} की {w} टन की {what} मिली। क्या आप इसे रद्द करना चाहते हैं?",
                "{dest} ಕಡೆಗೆ {w} ಟನ್‌ನ ನಿಮ್ಮ {what} ಸಿಕ್ಕಿದೆ. ಇದನ್ನು ರದ್ದುಗೊಳಿಸಬೇಕೇ?"),
 "cancel_done_driver": ("Your booking has been cancelled. Your truck is available again and I will look for another matching load.",
                        "आपकी बुकिंग रद्द हो गई। आपका ट्रक फिर उपलब्ध है और मैं दूसरा लोड ढूँढूँगा।",
                        "ನಿಮ್ಮ ಬುಕಿಂಗ್ ರದ್ದಾಗಿದೆ. ನಿಮ್ಮ ಟ್ರಕ್ ಮತ್ತೆ ಲಭ್ಯವಿದೆ, ಬೇರೆ ಲೋಡ್ ಹುಡುಕುತ್ತೇನೆ."),
 "cancel_done_trader": ("Your load has been cancelled. The matched truck is available again.",
                        "आपका लोड रद्द हो गया है। जुड़ा हुआ ट्रक फिर उपलब्ध है।",
                        "ನಿಮ್ಮ ಲೋಡ್ ರದ್ದಾಗಿದೆ. ಜೋಡಿಸಿದ್ದ ಟ್ರಕ್ ಮತ್ತೆ ಲಭ್ಯವಿದೆ."),
 "cancel_done_truck": ("Your truck listing has been cancelled.", "आपका ट्रक सूचीबद्ध होना रद्द कर दिया है।",
                       "ನಿಮ್ಮ ಟ್ರಕ್ ಪಟ್ಟಿ ರದ್ದಾಗಿದೆ."),
 "cancel_none": ("I could not find anything active to cancel.", "रद्द करने के लिए कुछ सक्रिय नहीं मिला।",
                 "ರದ್ದುಗೊಳಿಸಲು ಯಾವುದೇ ಸಕ್ರಿಯ ಬುಕಿಂಗ್ ಸಿಗಲಿಲ್ಲ."),
 "cancel_keep": ("Okay, nothing was cancelled.", "ठीक है, कुछ रद्द नहीं किया।", "ಸರಿ, ಏನನ್ನೂ ರದ್ದುಗೊಳಿಸಿಲ್ಲ."),
 "cancel_late": ("Sorry, this booking is already loaded or on the way, so it cannot be cancelled here.",
                 "क्षमा कीजिए, यह बुकिंग लोड हो चुकी है या रास्ते में है, इसे यहाँ रद्द नहीं कर सकते।",
                 "ಕ್ಷಮಿಸಿ, ಈ ಬುಕಿಂಗ್ ಈಗಾಗಲೇ ಲೋಡ್ ಆಗಿದೆ ಅಥವಾ ದಾರಿಯಲ್ಲಿದೆ, ಇಲ್ಲಿ ರದ್ದುಗೊಳಿಸಲು ಸಾಧ್ಯವಿಲ್ಲ."),
 "resched_q": ("What time do you need it? For example, tomorrow 9 AM.", "आपको कब चाहिए? जैसे, कल सुबह 9 बजे।",
               "ಯಾವಾಗ ಬೇಕು? ಉದಾಹರಣೆಗೆ, ನಾಳೆ ಬೆಳಿಗ್ಗೆ 9 ಗಂಟೆಗೆ."),
 "resched_load": ("Okay. I have changed your requirement to {when}. I will look for a suitable truck.",
                  "ठीक है। आपकी ज़रूरत {when} कर दी है। मैं उपयुक्त ट्रक ढूँढूँगा।",
                  "ಸರಿ. ನಿಮ್ಮ ಅವಶ್ಯಕತೆಯ ಸಮಯವನ್ನು ಬದಲಾಯಿಸಿದ್ದೇನೆ: {when}. ಸೂಕ್ತ ಟ್ರಕ್ ಹುಡುಕುತ್ತೇನೆ."),
 "resched_truck": ("Okay. I have changed your availability: your truck is available {when}. I will look for a suitable load.",
                   "ठीक है। आपकी उपलब्धता बदल दी है: ट्रक {when} उपलब्ध है। मैं उपयुक्त लोड ढूँढूँगा।",
                   "ಸರಿ. ನಿಮ್ಮ ಲಭ್ಯತೆಯನ್ನು ಬದಲಾಯಿಸಿದ್ದೇನೆ: ಟ್ರಕ್ {when} ಲಭ್ಯ. ಸೂಕ್ತ ಲೋಡ್ ಹುಡುಕುತ್ತೇನೆ."),
 "resched_none": ("I could not find an active request to change.", "बदलने के लिए कोई सक्रिय रिक्वेस्ट नहीं मिली।",
                  "ಬದಲಾಯಿಸಲು ಯಾವುದೇ ಸಕ್ರಿಯ ಮನವಿ ಸಿಗಲಿಲ್ಲ."),
 "status_loaded": ("Okay. I have updated your booking. Status: loaded. Have a safe journey.",
                   "ठीक है। बुकिंग अपडेट कर दी। स्थिति: लोड हो गया। सुरक्षित यात्रा हो।",
                   "ಸರಿ. ಬುಕಿಂಗ್ ಅಪ್‌ಡೇಟ್ ಮಾಡಿದ್ದೇನೆ. ಸ್ಥಿತಿ: ಲೋಡ್ ಆಗಿದೆ. ಸುರಕ್ಷಿತ ಪ್ರಯಾಣ."),
 "status_in_transit": ("Okay. Your trip status is now In Transit. Safe journey.",
                       "ठीक है। आपकी यात्रा की स्थिति अब रास्ते में है। सुरक्षित यात्रा हो।",
                       "ಸರಿ. ನಿಮ್ಮ ಟ್ರಿಪ್ ಸ್ಥಿತಿ ಈಗ ದಾರಿಯಲ್ಲಿದೆ. ಸುರಕ್ಷಿತ ಪ್ರಯಾಣ."),
 "status_delivered": ("Delivery recorded. Your trip is now completed. Thank you.",
                      "डिलीवरी दर्ज हो गई। आपकी ट्रिप पूरी हुई। धन्यवाद।",
                      "ಡೆಲಿವರಿ ದಾಖಲಾಗಿದೆ. ನಿಮ್ಮ ಟ್ರಿಪ್ ಪೂರ್ಣಗೊಂಡಿದೆ. ಧನ್ಯವಾದಗಳು."),
 "status_none": ("I could not find an active booking for you.", "आपकी कोई सक्रिय बुकिंग नहीं मिली।",
                 "ನಿಮ್ಮ ಯಾವುದೇ ಸಕ್ರಿಯ ಬುಕಿಂಗ್ ಸಿಗಲಿಲ್ಲ."),
 "st_loaded": ("loaded", "लोड हो गया", "ಲೋಡ್ ಆಗಿದೆ"),
 "st_in_transit": ("in transit", "रास्ते में", "ದಾರಿಯಲ್ಲಿದೆ"),
 "st_delivered": ("delivered", "डिलीवर हो गया", "ತಲುಪಿದೆ"),
 "rate_say": ("The current return-trip rate from {a} to {b} is about {rate} rupees per tonne, around {full} rupees for a full truck. Would you like me to look for a load on that route?",
              "{a} से {b} का अभी का वापसी भाड़ा लगभग {rate} रुपये प्रति टन है, पूरे ट्रक के लिए करीब {full} रुपये। क्या मैं इस रूट पर लोड ढूँढूँ?",
              "{a} ಇಂದ {b} ಕಡೆಗೆ ಈಗಿನ ವಾಪಸ್ ಟ್ರಿಪ್ ದರ ಸುಮಾರು ಟನ್‌ಗೆ {rate} ರೂಪಾಯಿ, ಪೂರ್ಣ ಟ್ರಕ್‌ಗೆ ಸುಮಾರು {full} ರೂಪಾಯಿ. ಈ ಮಾರ್ಗದಲ್ಲಿ ಲೋಡ್ ಹುಡುಕಬೇಕೇ?"),
 "rate_unknown": ("I don't have a rate for {a} to {b} yet.", "{a} से {b} का भाड़ा अभी मेरे पास नहीं है।",
                  "{a} ಇಂದ {b} ಕಡೆಗೆ ದರ ಇನ್ನೂ ನನ್ನ ಬಳಿ ಇಲ್ಲ."),
 "rate_which": ("Which route? Please tell me the destination.", "कौन सा रूट? कृपया गंतव्य बताइए।",
                "ಯಾವ ಮಾರ್ಗ? ದಯವಿಟ್ಟು ಗಮ್ಯಸ್ಥಾನ ಹೇಳಿ."),
 "unclear": ("No problem. I may have missed that. Could you please repeat it for me? ",
              "कोई बात नहीं। शायद मैं आपकी बात ठीक से नहीं समझ पाया। कृपया एक बार फिर बता दीजिए। ",
              "ಪರವಾಗಿಲ್ಲ. ನಿಮ್ಮ ಮಾತು ನನಗೆ ಸರಿಯಾಗಿ ಅರ್ಥವಾಗಿರಲಿಲ್ಲ. ದಯವಿಟ್ಟು ಮತ್ತೊಮ್ಮೆ ಹೇಳಿ. "),
 "human_offer": ("I'm sorry, I still didn't catch that. Would you like me to connect you with a human helper?",
                 "माफ़ कीजिए, मैं अभी भी आपकी बात ठीक से नहीं समझ पाया। क्या आप चाहेंगे कि मैं आपको किसी इंसान से जोड़ दूँ?",
                 "ಕ್ಷಮಿಸಿ, ನಿಮ್ಮ ಮಾತು ನನಗೆ ಇನ್ನೂ ಸರಿಯಾಗಿ ಅರ್ಥವಾಗಲಿಲ್ಲ. ಬೇಕಿದ್ದರೆ ನಿಮ್ಮನ್ನು ಒಬ್ಬ ಸಹಾಯಕರಿಗೆ ಸಂಪರ್ಕಿಸಬಹುದು. ಸಂಪರ್ಕಿಸಲೇ?"),
 "human_go": ("Okay. Please wait while I connect your call.", "ठीक है। कृपया रुकिए, मैं आपकी कॉल जोड़ रहा हूँ।",
              "ಸರಿ. ದಯವಿಟ್ಟು ಕಾಯಿರಿ, ನಿಮ್ಮ ಕರೆಯನ್ನು ಸಂಪರ್ಕಿಸುತ್ತಿದ್ದೇನೆ."),
 "anything_else": ("I'm happy to help. Is there anything else you need?",
                     "मैं मदद करने के लिए यहाँ हूँ। क्या आपको और किसी चीज़ की ज़रूरत है?",
                     "ನಾನು ಸಹಾಯ ಮಾಡಲು ಇದ್ದೇನೆ. ಇನ್ನೇನಾದರೂ ಸಹಾಯ ಬೇಕೇ?"),
 "bye": ("Thank you for calling Amargol Load Help. Drive safely.",
         "अमरगोल लोड हेल्प को कॉल करने के लिए धन्यवाद। सुरक्षित चलाइए।",
         "ಅಮರಗೋಳ ಲೋಡ್ ಹೆಲ್ಪ್‌ಗೆ ಕರೆ ಮಾಡಿದ್ದಕ್ಕೆ ಧನ್ಯವಾದಗಳು. ಸುರಕ್ಷಿತವಾಗಿ ಚಲಾಯಿಸಿ."),
 "cb_greet": ("Namaskara, this is Amargol Load Help calling you back.",
              "नमस्कार, मैं अमरगोल लोड हेल्प से आपको वापस फ़ोन कर रहा हूँ।",
              "ನಮಸ್ಕಾರ, ಅಮರಗೋಳ ಲೋಡ್ ಹೆಲ್ಪ್‌ನಿಂದ ನಿಮಗೆ ಮರಳಿ ಕರೆ ಮಾಡುತ್ತಿದ್ದೇನೆ."),
 "notice_confirmed": ("Good news. Your {dest} booking for {w} tonnes at {price} rupees is confirmed. The other party will contact you for pickup details.",
                      "खुशखबरी। {dest} की {w} टन की आपकी बुकिंग {price} रुपये पर पक्की हो गई है। पिकअप की जानकारी के लिए दूसरा पक्ष आपसे संपर्क करेगा।",
                      "ಸಿಹಿ ಸುದ್ದಿ. {dest} ಕಡೆಗೆ {w} ಟನ್‌ನ ನಿಮ್ಮ ಬುಕಿಂಗ್ {price} ರೂಪಾಯಿಗೆ ಖಚಿತವಾಗಿದೆ. ಪಿಕಪ್ ವಿವರಗಳಿಗಾಗಿ ಇನ್ನೊಂದು ಕಡೆಯವರು ನಿಮ್ಮನ್ನು ಸಂಪರ್ಕಿಸುತ್ತಾರೆ."),
 "notice_cancelled": ("Update: your {dest} booking was cancelled by the other party. Your request is active again and I will keep looking.",
                      "सूचना: आपकी {dest} की बुकिंग दूसरे पक्ष ने रद्द कर दी है। आपकी रिक्वेस्ट फिर सक्रिय है और मैं ढूँढता रहूँगा।",
                      "ಸೂಚನೆ: ನಿಮ್ಮ {dest} ಬುಕಿಂಗ್ ಅನ್ನು ಇನ್ನೊಂದು ಕಡೆಯವರು ರದ್ದುಗೊಳಿಸಿದ್ದಾರೆ. ನಿಮ್ಮ ಮನವಿ ಮತ್ತೆ ಸಕ್ರಿಯವಾಗಿದೆ, ಹುಡುಕುತ್ತಲೇ ಇರುತ್ತೇನೆ."),
 "notice_declined": ("Update: the other party declined the {dest} offer. Your request is still active and I will keep looking.",
                     "सूचना: दूसरे पक्ष ने {dest} का ऑफ़र नहीं माना। आपकी रिक्वेस्ट सक्रिय है और मैं ढूँढता रहूँगा।",
                     "ಸೂಚನೆ: {dest} ಆಫರ್ ಅನ್ನು ಇನ್ನೊಂದು ಕಡೆಯವರು ಒಪ್ಪಲಿಲ್ಲ. ನಿಮ್ಮ ಮನವಿ ಸಕ್ರಿಯವಾಗಿದೆ, ಹುಡುಕುತ್ತಲೇ ಇರುತ್ತೇನೆ."),
 "notice_rescheduled": ("Update: the other party changed the time for your {dest} booking to {when}.",
                        "सूचना: दूसरे पक्ष ने आपकी {dest} बुकिंग का समय बदलकर {when} कर दिया है।",
                        "ಸೂಚನೆ: ನಿಮ್ಮ {dest} ಬುಕಿಂಗ್‌ನ ಸಮಯವನ್ನು ಇನ್ನೊಂದು ಕಡೆಯವರು ಬದಲಾಯಿಸಿದ್ದಾರೆ: {when}."),
 "notice_status": ("Update: your {dest} load is now {st}.", "सूचना: आपका {dest} का लोड अब {st}।",
                   "ಸೂಚನೆ: ನಿಮ್ಮ {dest} ಲೋಡ್ ಈಗ {st}."),
 "notice_expired": ("Update: your {dest} request has expired without a match. Please call again to post it again.",
                    "सूचना: आपकी {dest} की रिक्वेस्ट बिना मैच के समाप्त हो गई। दोबारा दर्ज करने के लिए फिर से कॉल कीजिए।",
                    "ಸೂಚನೆ: ನಿಮ್ಮ {dest} ಮನವಿ ಹೊಂದಾಣಿಕೆ ಸಿಗದೆ ಮುಗಿದಿದೆ. ಮತ್ತೆ ದಾಖಲಿಸಲು ದಯವಿಟ್ಟು ಮತ್ತೊಮ್ಮೆ ಕರೆ ಮಾಡಿ."),
}


def n(x):
    x = float(x)
    return str(int(x)) if x.is_integer() else "%g" % x


def dur_str(h, lang):
    if 0.9 <= h < 1:
        h = 1
    v, k = (max(1, round(h * 60)), "m") if h < 1 else (round(h * 2) / 2, "h")
    v = n(v)
    if lang == "hi":
        return "%s %s" % (v, "मिनट" if k == "m" else "घंटे")
    if lang == "kn":
        return "%s %s" % (v, "ನಿಮಿಷ" if k == "m" else "ಗಂಟೆ")
    return "%s %s%s" % (v, "minute" if k == "m" else "hour", "" if float(v) == 1 else "s")


def when_str(ts, lang):
    now = time.time()
    if ts - now <= 6 * 3600:
        d = dur_str(max(ts - now, 1800) / 3600, lang)
        if lang == "hi":
            return d + " के अंदर"
        if lang == "kn":
            return d + " ಒಳಗೆ"
        return "within " + d
    dt = datetime.fromtimestamp(ts, IST)
    days = (dt.date() - datetime.now(IST).date()).days
    day = {
        "en": {0: "today", 1: "tomorrow"},
        "hi": {0: "आज", 1: "कल"},
        "kn": {0: "ಇಂದು", 1: "ನಾಳೆ"},
    }.get(lang, {0: "today", 1: "tomorrow"}).get(days, dt.strftime("%d %b"))
    hm = dt.strftime("%H:%M")
    if lang == "hi":
        return "%s %s बजे" % (day, hm)
    if lang == "kn":
        return "%s %sಕ್ಕೆ" % (day, hm)
    return "%s at %s" % (day, hm)


MULTI = {
    "en": {
        "q_dest_load": "Where does the load need to go?",
        "q_dest_truck": "Where is your truck going?",
        "unclear": "No problem. I may have missed that. Could you please repeat it for me? ",
        "got_it": "Thanks, got it. ",
        "dest_unrecognized": "I didn't recognize that place. Could you please repeat the destination? ",
        "dest_unavailable": "Sorry, I can't deliver to that location right now. Here are the destinations where trucks are currently available. Please tell me the number. ",
        "dest_choose": "Please tell me one number from the list. ",
        "dest_bad_number": "That number is not available. Please tell me one number from the list. ",
        "dest_none": "I don't have any currently available truck destinations right now. Could you please tell me another destination? ",
    },
    "hi": {
        "q_dest_load": "लोड कहाँ भेजना है?", "q_dest_truck": "आपका ट्रक कहाँ जा रहा है?",
        "unclear": "कोई बात नहीं। शायद मैं आपकी बात ठीक से नहीं समझ पाया। कृपया एक बार फिर बता दीजिए। ",
        "got_it": "धन्यवाद, समझ गया। ",
        "dest_unrecognized": "मुझे वह जगह समझ नहीं आई। कृपया गंतव्य एक बार फिर बताइए। ",
        "dest_unavailable": "माफ़ कीजिए, उस जगह के लिए अभी डिलीवरी उपलब्ध नहीं है। ये वे गंतव्य हैं जहाँ अभी ट्रक उपलब्ध हैं। कृपया नंबर बताइए। ",
        "dest_choose": "कृपया सूची में से एक नंबर बताइए। ",
        "dest_bad_number": "वह नंबर उपलब्ध नहीं है। कृपया सूची में से एक नंबर बताइए। ",
        "dest_none": "अभी कोई उपलब्ध ट्रक गंतव्य नहीं है। कृपया कोई दूसरा गंतव्य बताइए। ",
    },
    "kn": {
        "q_dest_load": "ಲೋಡ್ ಎಲ್ಲಿಗೆ ಹೋಗಬೇಕು?", "q_dest_truck": "ನಿಮ್ಮ ಟ್ರಕ್ ಎಲ್ಲಿಗೆ ಹೋಗುತ್ತಿದೆ?",
        "unclear": "ಪರವಾಗಿಲ್ಲ. ನಿಮ್ಮ ಮಾತು ನನಗೆ ಸರಿಯಾಗಿ ಅರ್ಥವಾಗಿರಲಿಲ್ಲ. ದಯವಿಟ್ಟು ಮತ್ತೊಮ್ಮೆ ಹೇಳಿ. ",
        "got_it": "ಧನ್ಯವಾದಗಳು, ಅರ್ಥವಾಯಿತು. ",
        "dest_unrecognized": "ಆ ಸ್ಥಳ ನನಗೆ ಗುರುತಾಗಲಿಲ್ಲ. ದಯವಿಟ್ಟು ಗಮ್ಯಸ್ಥಾನವನ್ನು ಮತ್ತೊಮ್ಮೆ ಹೇಳಿ. ",
        "dest_unavailable": "ಕ್ಷಮಿಸಿ, ಆ ಸ್ಥಳಕ್ಕೆ ಈಗ ಡೆಲಿವರಿ ಲಭ್ಯವಿಲ್ಲ. ಈಗ ಟ್ರಕ್‌ಗಳು ಲಭ್ಯವಿರುವ ಗಮ್ಯಸ್ಥಾನಗಳು ಇವು. ದಯವಿಟ್ಟು ಸಂಖ್ಯೆಯನ್ನು ಹೇಳಿ. ",
        "dest_choose": "ದಯವಿಟ್ಟು ಪಟ್ಟಿಯಲ್ಲಿರುವ ಒಂದು ಸಂಖ್ಯೆಯನ್ನು ಮಾತ್ರ ಹೇಳಿ. ",
        "dest_bad_number": "ಆ ಸಂಖ್ಯೆ ಲಭ್ಯವಿಲ್ಲ. ದಯವಿಟ್ಟು ಪಟ್ಟಿಯಲ್ಲಿರುವ ಒಂದು ಸಂಖ್ಯೆಯನ್ನು ಹೇಳಿ. ",
        "dest_none": "ಈಗ ಯಾವುದೇ ಲಭ್ಯ ಟ್ರಕ್ ಗಮ್ಯಸ್ಥಾನಗಳಿಲ್ಲ. ದಯವಿಟ್ಟು ಬೇರೆ ಗಮ್ಯಸ್ಥಾನವನ್ನು ಹೇಳಿ. ",
    },
    "gu": {
        "q_dest_load": "લોડ ક્યાં મોકલવાનો છે?", "q_dest_truck": "તમારી ટ્રક ક્યાં જઈ રહી છે?",
        "unclear": "કોઈ વાત નહીં. કદાચ હું તમારી વાત બરાબર સમજી શક્યો નથી. કૃપા કરીને ફરી એક વાર કહો. ",
        "got_it": "આભાર, સમજાઈ ગયું. ",
        "dest_unrecognized": "મને આ સ્થળ સમજાયું નથી. કૃપા કરીને ગંતવ્ય ફરી કહો. ",
        "dest_unavailable": "માફ કરશો, હાલમાં આ સ્થળ માટે ડિલિવરી ઉપલબ્ધ નથી. હાલમાં જ્યાં ટ્રક ઉપલબ્ધ છે તે ગંતવ્યો આ છે. કૃપા કરીને નંબર કહો. ",
        "dest_choose": "કૃપા કરીને યાદીમાંથી એક નંબર કહો. ",
        "dest_bad_number": "આ નંબર ઉપલબ્ધ નથી. કૃપા કરીને યાદીમાંથી એક નંબર કહો. ",
        "dest_none": "હાલમાં કોઈ ટ્રક ગંતવ્ય ઉપલબ્ધ નથી. કૃપા કરીને બીજું ગંતવ્ય કહો. ",
    },
    "mr": {
        "q_dest_load": "लोड कुठे पाठवायचा आहे?", "q_dest_truck": "तुमचा ट्रक कुठे जात आहे?",
        "unclear": "काही हरकत नाही. कदाचित मला तुमचे म्हणणे नीट समजले नाही. कृपया पुन्हा सांगा. ",
        "got_it": "धन्यवाद, समजले. ",
        "dest_unrecognized": "हे ठिकाण मला समजले नाही. कृपया गंतव्य पुन्हा सांगा. ",
        "dest_unavailable": "माफ करा, सध्या त्या ठिकाणी डिलिव्हरी उपलब्ध नाही. सध्या ट्रक उपलब्ध असलेली ठिकाणे ही आहेत. कृपया क्रमांक सांगा. ",
        "dest_choose": "कृपया यादीतील एक क्रमांक सांगा. ",
        "dest_bad_number": "हा क्रमांक उपलब्ध नाही. कृपया यादीतील एक क्रमांक सांगा. ",
        "dest_none": "सध्या कोणतेही उपलब्ध ट्रक गंतव्य नाही. कृपया दुसरे गंतव्य सांगा. ",
    },
    "ta": {
        "q_dest_load": "லோடு எங்கே செல்ல வேண்டும்?", "q_dest_truck": "உங்கள் லாரி எங்கே செல்கிறது?",
        "unclear": "பரவாயில்லை. உங்கள் பதிலை நான் சரியாகப் புரிந்துகொள்ளவில்லை. தயவுசெய்து மீண்டும் சொல்லுங்கள். ",
        "got_it": "நன்றி, புரிந்துகொண்டேன். ",
        "dest_unrecognized": "அந்த இடத்தை நான் புரிந்துகொள்ளவில்லை. தயவுசெய்து இலக்கை மீண்டும் சொல்லுங்கள். ",
        "dest_unavailable": "மன்னிக்கவும், அந்த இடத்திற்கு தற்போது டெலிவரி கிடைக்கவில்லை. தற்போது லாரிகள் கிடைக்கும் இடங்கள் இவை. தயவுசெய்து எண்ணை சொல்லுங்கள். ",
        "dest_choose": "பட்டியலில் இருந்து ஒரு எண்ணை மட்டும் சொல்லுங்கள். ",
        "dest_bad_number": "அந்த எண் கிடைக்கவில்லை. பட்டியலில் இருந்து ஒரு எண்ணை சொல்லுங்கள். ",
        "dest_none": "தற்போது லாரிகள் கிடைக்கும் இலக்குகள் இல்லை. வேறு இலக்கை சொல்லுங்கள். ",
    },
    "te": {
        "q_dest_load": "లోడ్ ఎక్కడికి వెళ్లాలి?", "q_dest_truck": "మీ ట్రక్ ఎక్కడికి వెళ్తోంది?",
        "unclear": "పర్లేదు. మీ మాట నాకు సరిగ్గా అర్థం కాలేదు. దయచేసి మళ్లీ చెప్పండి. ",
        "got_it": "ధన్యవాదాలు, అర్థమైంది. ",
        "dest_unrecognized": "ఆ ప్రదేశం నాకు అర్థం కాలేదు. దయచేసి గమ్యస్థానాన్ని మళ్లీ చెప్పండి. ",
        "dest_unavailable": "క్షమించండి, ప్రస్తుతం ఆ ప్రదేశానికి డెలివరీ అందుబాటులో లేదు. ప్రస్తుతం ట్రక్కులు అందుబాటులో ఉన్న గమ్యస్థానాలు ఇవి. దయచేసి నంబర్ చెప్పండి. ",
        "dest_choose": "జాబితాలోని ఒక నంబర్ మాత్రమే చెప్పండి. ",
        "dest_bad_number": "ఆ నంబర్ అందుబాటులో లేదు. జాబితాలోని ఒక నంబర్ చెప్పండి. ",
        "dest_none": "ప్రస్తుతం ట్రక్కులు అందుబాటులో ఉన్న గమ్యస్థానాలు లేవు. దయచేసి మరో గమ్యస్థానం చెప్పండి. ",
    },
    "ml": {
        "q_dest_load": "ലോഡ് എവിടേക്കാണ് പോകേണ്ടത്?", "q_dest_truck": "നിങ്ങളുടെ ട്രക്ക് എവിടേക്കാണ് പോകുന്നത്?",
        "unclear": "സാരമില്ല. നിങ്ങളുടെ മറുപടി എനിക്ക് ശരിയായി മനസ്സിലായില്ല. ദയവായി വീണ്ടും പറയൂ. ",
        "got_it": "നന്ദി, മനസ്സിലായി. ",
        "dest_unrecognized": "ആ സ്ഥലം എനിക്ക് മനസ്സിലായില്ല. ദയവായി ലക്ഷ്യസ്ഥലം വീണ്ടും പറയൂ. ",
        "dest_unavailable": "ക്ഷമിക്കണം, ഇപ്പോൾ ആ സ്ഥലത്തേക്ക് ഡെലിവറി ലഭ്യമല്ല. ഇപ്പോൾ ട്രക്കുകൾ ലഭ്യമായ സ്ഥലങ്ങൾ ഇവയാണ്. ദയവായി നമ്പർ പറയൂ. ",
        "dest_choose": "പട്ടികയിൽ നിന്ന് ഒരു നമ്പർ മാത്രം പറയൂ. ",
        "dest_bad_number": "ആ നമ്പർ ലഭ്യമല്ല. പട്ടികയിൽ നിന്ന് ഒരു നമ്പർ പറയൂ. ",
        "dest_none": "ഇപ്പോൾ ലഭ്യമായ ട്രക്ക് ലക്ഷ്യസ്ഥാനങ്ങളില്ല. ദയവായി മറ്റൊരു ലക്ഷ്യസ്ഥാനം പറയൂ. ",
    },
    "bn": {
        "q_dest_load": "লোড কোথায় পাঠাতে হবে?", "q_dest_truck": "আপনার ট্রাক কোথায় যাচ্ছে?",
        "unclear": "কোনও সমস্যা নেই। হয়তো আমি আপনার কথা ঠিকভাবে বুঝতে পারিনি। দয়া করে আবার বলুন। ",
        "got_it": "ধন্যবাদ, বুঝেছি। ",
        "dest_unrecognized": "আমি সেই জায়গাটি বুঝতে পারিনি। দয়া করে গন্তব্যটি আবার বলুন। ",
        "dest_unavailable": "দুঃখিত, এই মুহূর্তে ওই জায়গায় ডেলিভারি পাওয়া যাচ্ছে না। বর্তমানে ট্রাক পাওয়া যায় এমন গন্তব্যগুলি এগুলি। দয়া করে নম্বর বলুন। ",
        "dest_choose": "তালিকা থেকে একটি নম্বর বলুন। ",
        "dest_bad_number": "ওই নম্বরটি পাওয়া যাচ্ছে না। তালিকা থেকে একটি নম্বর বলুন। ",
        "dest_none": "এই মুহূর্তে কোনও ট্রাকের গন্তব্য উপলব্ধ নেই। দয়া করে অন্য গন্তব্য বলুন। ",
    },
    "pa": {
        "q_dest_load": "ਲੋਡ ਕਿੱਥੇ ਜਾਣਾ ਹੈ?", "q_dest_truck": "ਤੁਹਾਡਾ ਟਰੱਕ ਕਿੱਥੇ ਜਾ ਰਿਹਾ ਹੈ?",
        "unclear": "ਕੋਈ ਗੱਲ ਨਹੀਂ। ਸ਼ਾਇਦ ਮੈਂ ਤੁਹਾਡੀ ਗੱਲ ਠੀਕ ਤਰ੍ਹਾਂ ਨਹੀਂ ਸਮਝਿਆ। ਕਿਰਪਾ ਕਰਕੇ ਦੁਬਾਰਾ ਦੱਸੋ। ",
        "got_it": "ਧੰਨਵਾਦ, ਸਮਝ ਗਿਆ। ",
        "dest_unrecognized": "ਮੈਨੂੰ ਉਹ ਥਾਂ ਸਮਝ ਨਹੀਂ ਆਈ। ਕਿਰਪਾ ਕਰਕੇ ਮੰਜ਼ਿਲ ਦੁਬਾਰਾ ਦੱਸੋ। ",
        "dest_unavailable": "ਮਾਫ਼ ਕਰਨਾ, ਇਸ ਵੇਲੇ ਉਸ ਥਾਂ ਲਈ ਡਿਲਿਵਰੀ ਉਪਲਬਧ ਨਹੀਂ ਹੈ। ਇਸ ਵੇਲੇ ਟਰੱਕ ਉਪਲਬਧ ਮੰਜ਼ਿਲਾਂ ਇਹ ਹਨ। ਕਿਰਪਾ ਕਰਕੇ ਨੰਬਰ ਦੱਸੋ। ",
        "dest_choose": "ਕਿਰਪਾ ਕਰਕੇ ਸੂਚੀ ਵਿੱਚੋਂ ਇੱਕ ਨੰਬਰ ਦੱਸੋ। ",
        "dest_bad_number": "ਉਹ ਨੰਬਰ ਉਪਲਬਧ ਨਹੀਂ ਹੈ। ਸੂਚੀ ਵਿੱਚੋਂ ਇੱਕ ਨੰਬਰ ਦੱਸੋ। ",
        "dest_none": "ਇਸ ਵੇਲੇ ਕੋਈ ਟਰੱਕ ਮੰਜ਼ਿਲ ਉਪਲਬਧ ਨਹੀਂ ਹੈ। ਕਿਰਪਾ ਕਰਕੇ ਹੋਰ ਮੰਜ਼ਿਲ ਦੱਸੋ। ",
    },
    "or": {
        "q_dest_load": "ଲୋଡ୍ କେଉଁଠାକୁ ଯିବାକୁ ହେବ?", "q_dest_truck": "ଆପଣଙ୍କ ଟ୍ରକ୍ କେଉଁଠାକୁ ଯାଉଛି?",
        "unclear": "କିଛି ଅସୁବିଧା ନାହିଁ। ବୋଧହୁଏ ମୁଁ ଆପଣଙ୍କ କଥା ଠିକ୍ ଭାବରେ ବୁଝିପାରିଲି ନାହିଁ। ଦୟାକରି ପୁଣି କହନ୍ତୁ। ",
        "got_it": "ଧନ୍ୟବାଦ, ବୁଝିଲି। ",
        "dest_unrecognized": "ସେହି ସ୍ଥାନଟି ମୁଁ ବୁଝିପାରିଲି ନାହିଁ। ଦୟାକରି ଗନ୍ତବ୍ୟସ୍ଥଳ ପୁଣି କହନ୍ତୁ। ",
        "dest_unavailable": "ଦୁଃଖିତ, ବର୍ତ୍ତମାନ ସେହି ସ୍ଥାନକୁ ଡେଲିଭରି ଉପଲବ୍ଧ ନାହିଁ। ବର୍ତ୍ତମାନ ଟ୍ରକ୍ ଉପଲବ୍ଧ ଥିବା ଗନ୍ତବ୍ୟଗୁଡ଼ିକ ଏଗୁଡ଼ିକ। ଦୟାକରି ନମ୍ବର କହନ୍ତୁ। ",
        "dest_choose": "ତାଲିକାରୁ ଗୋଟିଏ ନମ୍ବର କହନ୍ତୁ। ",
        "dest_bad_number": "ସେହି ନମ୍ବର ଉପଲବ୍ଧ ନାହିଁ। ତାଲିକାରୁ ଗୋଟିଏ ନମ୍ବର କହନ୍ତୁ। ",
        "dest_none": "ବର୍ତ୍ତମାନ କୌଣସି ଟ୍ରକ୍ ଗନ୍ତବ୍ୟ ଉପଲବ୍ଧ ନାହିଁ। ଦୟାକରି ଅନ୍ୟ ଗନ୍ତବ୍ୟ କହନ୍ତୁ। ",
    },
    "as": {
        "q_dest_load": "লোড ক'লৈ যাব লাগে?", "q_dest_truck": "আপোনাৰ ট্ৰাক ক'লৈ গৈ আছে?",
        "unclear": "কোনো কথা নাই। হয়তো মই আপোনাৰ কথা ভালদৰে বুজি পোৱা নাই। অনুগ্ৰহ কৰি আকৌ কওক। ",
        "got_it": "ধন্যবাদ, বুজিলোঁ। ",
        "dest_unrecognized": "মই সেই ঠাইটো বুজি পোৱা নাই। অনুগ্ৰহ কৰি গন্তব্য আকৌ কওক। ",
        "dest_unavailable": "দুঃখিত, এই মুহূৰ্তত সেই ঠাইলৈ ডেলিভাৰী উপলব্ধ নহয়। বৰ্তমান ট্ৰাক উপলব্ধ থকা গন্তব্যসমূহ এইবোৰ। অনুগ্ৰহ কৰি নম্বৰ কওক। ",
        "dest_choose": "অনুগ্ৰহ কৰি তালিকাৰ পৰা এটা নম্বৰ কওক। ",
        "dest_bad_number": "সেই নম্বৰটো উপলব্ধ নহয়। তালিকাৰ পৰা এটা নম্বৰ কওক। ",
        "dest_none": "এই মুহূৰ্তত কোনো ট্ৰাক গন্তব্য উপলব্ধ নাই। অনুগ্ৰহ কৰি আন এটা গন্তব্য কওক। ",
    },
}

SLOT_MULTI = {
    "gu": {"q_cap":"તમારી ટ્રકમાં કેટલી જગ્યા ખાલી છે?", "q_hours":"લોડ માટે તમે કેટલો સમય રાહ જોઈ શકો છો?", "q_price":"આ ટ્રિપ માટે તમે કેટલું ભાડું ઇચ્છો છો?", "q_weight":"લોડનું વજન કેટલું છે?", "q_when":"તમને ટ્રક ક્યારે જોઈએ છે?"},
    "mr": {"q_cap":"तुमच्या ट्रकमध्ये किती जागा रिकामी आहे?", "q_hours":"लोडसाठी तुम्ही किती वेळ थांबू शकता?", "q_price":"या ट्रिपसाठी तुम्हाला किती भाडे हवे आहे?", "q_weight":"लोडचे वजन किती आहे?", "q_when":"तुम्हाला ट्रक कधी हवा आहे?"},
    "ta": {"q_cap":"உங்கள் லாரியில் எவ்வளவு இடம் காலியாக உள்ளது?", "q_hours":"லோடுக்காக எவ்வளவு நேரம் காத்திருக்க முடியும்?", "q_price":"இந்த பயணத்திற்கு எவ்வளவு கட்டணம் எதிர்பார்க்கிறீர்கள்?", "q_weight":"லோடின் எடை எவ்வளவு?", "q_when":"உங்களுக்கு லாரி எப்போது வேண்டும்?"},
    "te": {"q_cap":"మీ ట్రక్‌లో ఎంత స్థలం ఖాళీగా ఉంది?", "q_hours":"లోడ్ కోసం ఎంతసేపు వేచి ఉండగలరు?", "q_price":"ఈ ట్రిప్‌కు ఎంత ఛార్జీ కావాలి?", "q_weight":"లోడ్ బరువు ఎంత?", "q_when":"మీకు ట్రక్ ఎప్పుడు కావాలి?"},
    "ml": {"q_cap":"നിങ്ങളുടെ ട്രക്കിൽ എത്ര സ്ഥലം ഒഴിഞ്ഞുകിടക്കുന്നു?", "q_hours":"ലോഡിനായി എത്ര നേരം കാത്തിരിക്കാം?", "q_price":"ഈ യാത്രയ്ക്ക് എത്ര കൂലി വേണം?", "q_weight":"ലോഡിന്റെ ഭാരം എത്രയാണ്?", "q_when":"നിങ്ങൾക്ക് ട്രക്ക് എപ്പോൾ വേണം?"},
    "bn": {"q_cap":"আপনার ট্রাকে কতটা জায়গা খালি আছে?", "q_hours":"লোডের জন্য কতক্ষণ অপেক্ষা করতে পারবেন?", "q_price":"এই ট্রিপের জন্য কত ভাড়া চান?", "q_weight":"লোডের ওজন কত?", "q_when":"আপনার ট্রাক কখন দরকার?"},
    "pa": {"q_cap":"ਤੁਹਾਡੇ ਟਰੱਕ ਵਿੱਚ ਕਿੰਨੀ ਜਗ੍ਹਾ ਖਾਲੀ ਹੈ?", "q_hours":"ਲੋਡ ਲਈ ਤੁਸੀਂ ਕਿੰਨੀ ਦੇਰ ਉਡੀਕ ਕਰ ਸਕਦੇ ਹੋ?", "q_price":"ਇਸ ਟ੍ਰਿਪ ਲਈ ਤੁਸੀਂ ਕਿੰਨਾ ਕਿਰਾਇਆ ਚਾਹੁੰਦੇ ਹੋ?", "q_weight":"ਲੋਡ ਦਾ ਵਜ਼ਨ ਕਿੰਨਾ ਹੈ?", "q_when":"ਤੁਹਾਨੂੰ ਟਰੱਕ ਕਦੋਂ ਚਾਹੀਦਾ ਹੈ?"},
    "or": {"q_cap":"ଆପଣଙ୍କ ଟ୍ରକ୍‌ରେ କେତେ ଜାଗା ଖାଲି ଅଛି?", "q_hours":"ଲୋଡ୍ ପାଇଁ ଆପଣ କେତେ ସମୟ ଅପେକ୍ଷା କରିପାରିବେ?", "q_price":"ଏହି ଟ୍ରିପ୍ ପାଇଁ ଆପଣ କେତେ ଭଡା ଚାହୁଁଛନ୍ତି?", "q_weight":"ଲୋଡ୍‌ର ଓଜନ କେତେ?", "q_when":"ଆପଣଙ୍କୁ ଟ୍ରକ୍ କେବେ ଦରକାର?"},
    "as": {"q_cap":"আপোনাৰ ট্ৰাকত কিমান ঠাই খালী আছে?", "q_hours":"লোডৰ বাবে কিমান সময় অপেক্ষা কৰিব পাৰিব?", "q_price":"এই যাত্ৰাৰ বাবে কিমান ভাড়া বিচাৰে?", "q_weight":"লোডৰ ওজন কিমান?", "q_when":"আপোনাক ট্ৰাক কেতিয়া লাগে?"},
}

HUMAN_MULTI = {
    "gu": "માફ કરશો, હું હજુ પણ તમારી વાત સમજી શક્યો નથી. શું હું તમને માનવીય સહાયક સાથે જોડું?",
    "mr": "माफ करा, मला अजूनही तुमचे म्हणणे समजले नाही. मी तुम्हाला मानवी सहाय्यकाशी जोडू का?",
    "ta": "மன்னிக்கவும், உங்கள் பேச்சு எனக்கு இன்னும் புரியவில்லை. உங்களை ஒரு உதவியாளருடன் இணைக்கவா?",
    "te": "క్షమించండి, మీ మాట నాకు ఇంకా అర్థం కాలేదు. మిమ్మల్ని ఒక సహాయకుడితో కలపనా?",
    "ml": "ക്ഷമിക്കണം, നിങ്ങളുടെ വാക്കുകൾ എനിക്ക് ഇപ്പോഴും മനസ്സിലായില്ല. നിങ്ങളെ ഒരു സഹായിയുമായി ബന്ധിപ്പിക്കട്ടേ?",
    "bn": "দুঃখিত, আমি এখনও আপনার কথা বুঝতে পারছি না। আপনাকে কি একজন সহায়কের সঙ্গে যুক্ত করব?",
    "pa": "ਮਾਫ਼ ਕਰਨਾ, ਮੈਨੂੰ ਅਜੇ ਵੀ ਤੁਹਾਡੀ ਗੱਲ ਸਮਝ ਨਹੀਂ ਆਈ। ਕੀ ਮੈਂ ਤੁਹਾਨੂੰ ਕਿਸੇ ਸਹਾਇਕ ਨਾਲ ਜੋੜਾਂ?",
    "or": "ଦୁଃଖିତ, ମୁଁ ଏପର୍ଯ୍ୟନ୍ତ ଆପଣଙ୍କ କଥା ବୁଝିପାରିଲି ନାହିଁ। ଆପଣଙ୍କୁ ଜଣେ ସହାୟକଙ୍କ ସହିତ ଯୋଡ଼ିଦେବି କି?",
    "as": "দুঃখিত, মই এতিয়াও আপোনাৰ কথা বুজি পোৱা নাই। আপোনাক এজন সহায়কৰ সৈতে সংযোগ কৰিম নেকি?",
}

CITY_LOCAL = {
    "gu": {"Bengaluru":"બેંગલુરુ","Hyderabad":"હૈદરાબાદ","Mysuru":"મૈસુરુ","Mandya":"માંડ્યા","Hubballi":"હુબ્બલ્લી","Dharwad":"ધારવાડ","Belagavi":"બેલગાવી","Mangaluru":"મેંગલુરુ","Pune":"પુણે","Mumbai":"મુંબઈ","Chennai":"ચેન્નાઈ"},
    "mr": {"Bengaluru":"बेंगळुरू","Hyderabad":"हैदराबाद","Mysuru":"म्हैसूर","Hubballi":"हुबळी","Dharwad":"धारवाड","Belagavi":"बेळगावी","Pune":"पुणे","Mumbai":"मुंबई","Chennai":"चेन्नई"},
    "ta": {"Bengaluru":"பெங்களூரு","Hyderabad":"ஹைதராபாத்","Mysuru":"மைசூரு","Hubballi":"ஹுப்பள்ளி","Dharwad":"தார்வாட்","Belagavi":"பெலகாவி","Pune":"புனே","Mumbai":"மும்பை","Chennai":"சென்னை"},
    "te": {"Bengaluru":"బెంగళూరు","Hyderabad":"హైదరాబాద్","Mysuru":"మైసూరు","Hubballi":"హుబ్బల్లి","Dharwad":"ధార్వాడ్","Belagavi":"బెలగావి","Pune":"పుణే","Mumbai":"ముంబై","Chennai":"చెన్నై"},
    "ml": {"Bengaluru":"ബെംഗളൂരു","Hyderabad":"ഹൈദരാബാദ്","Mysuru":"മൈസൂരു","Hubballi":"ഹുബ്ബള്ളി","Dharwad":"ധാർവാഡ്","Belagavi":"ബെളഗാവി","Pune":"പൂനെ","Mumbai":"മുംബൈ","Chennai":"ചെന്നൈ"},
    "bn": {"Bengaluru":"বেঙ্গালুরু","Hyderabad":"হায়দরাবাদ","Mysuru":"মাইসুরু","Hubballi":"হুব্বালি","Dharwad":"ধারওয়াড়","Belagavi":"বেলগাভি","Pune":"পুনে","Mumbai":"মুম্বাই","Chennai":"চেন্নাই"},
    "pa": {"Bengaluru":"ਬੈਂਗਲੁਰੂ","Hyderabad":"ਹੈਦਰਾਬਾਦ","Mysuru":"ਮੈਸੂਰ","Hubballi":"ਹੁਬਲੀ","Dharwad":"ਧਾਰਵਾੜ","Belagavi":"ਬੇਲਗਾਵੀ","Pune":"ਪੁਣੇ","Mumbai":"ਮੁੰਬਈ","Chennai":"ਚੇਨਈ"},
    "or": {"Bengaluru":"ବେଙ୍ଗାଲୁରୁ","Hyderabad":"ହାଇଦ୍ରାବାଦ","Mysuru":"ମୈସୁରୁ","Hubballi":"ହୁବ୍ବାଲି","Dharwad":"ଧାରୱାଡ","Belagavi":"ବେଲଗାଭି","Pune":"ପୁଣେ","Mumbai":"ମୁମ୍ବାଇ","Chennai":"ଚେନ୍ନାଇ"},
    "as": {"Bengaluru":"বেংগালুৰু","Hyderabad":"হায়দৰাবাদ","Mysuru":"মাইচুৰু","Hubballi":"হুব্বালি","Dharwad":"ধাৰৱাড়","Belagavi":"বেলগাভি","Pune":"পুনে","Mumbai":"মুম্বাই","Chennai":"চেন্নাই"},
}

def display_city(c, lang):
    if lang in CITY_LOCAL and c in CITY_LOCAL[lang]:
        return CITY_LOCAL[lang][c]
    return city_name(c, lang)


SLOT_SPECIFIC_TIME = {
    "en": "What specific time do you need the truck?",
    "hi": "आपको ट्रक किस समय चाहिए?",
    "kn": "ನಿಮಗೆ ಟ್ರಕ್ ಯಾವ ಸಮಯಕ್ಕೆ ಬೇಕು?",
    "ta": "உங்களுக்கு லாரி எந்த குறிப்பிட்ட நேரத்தில் வேண்டும்?",
    "te": "మీకు ట్రక్ ఏ నిర్దిష్ట సమయానికి కావాలి?",
    "ml": "നിങ്ങൾക്ക് ട്രക്ക് ഏത് കൃത്യമായ സമയത്താണ് വേണ്ടത്?",
    "mr": "तुम्हाला ट्रक नेमक्या कोणत्या वेळी हवा आहे?",
    "gu": "તમને ટ્રક ચોક્કસ કયા સમયે જોઈએ છે?",
    "bn": "আপনার ট্রাক ঠিক কোন সময়ে দরকার?",
    "pa": "ਤੁਹਾਨੂੰ ਟਰੱਕ ਬਿਲਕੁਲ ਕਿਸ ਸਮੇਂ ਚਾਹੀਦਾ ਹੈ?",
    "or": "ଆପଣଙ୍କୁ ଟ୍ରକ୍ ଠିକ୍ କେଉଁ ସମୟରେ ଦରକାର?",
    "as": "আপোনাক ট্ৰাক ঠিক কোন সময়ত লাগে?",
}


def active_destination_options(lang):
    rows = q("SELECT DISTINCT destination FROM trucks WHERE status='available' AND available_until>? ORDER BY destination", (now(),))
    seen = []
    for r in rows:
        d = r["destination"]
        if d in CITIES and d != HUB and d not in seen:
            seen.append(d)
    return seen

def destination_list_text(s):
    options = active_destination_options(s.lang)
    s.dest_options = options
    if not options:
        return T(s.lang, "dest_none")
    lines = ["%d. %s" % (i + 1, display_city(d, s.lang)) for i, d in enumerate(options)]
    return T(s.lang, "dest_unavailable") + "\n" + "\n".join(lines)


LOGISTICS_ONLY = {
    "en": "I don't have information about that. Could you need help with the logistics?",
    "hi": "मेरे पास उस विषय की जानकारी नहीं है। क्या आपको लॉजिस्टिक्स में मदद चाहिए?",
    "kn": "ಅದರ ಬಗ್ಗೆ ನನ್ನ ಬಳಿ ಮಾಹಿತಿ ಇಲ್ಲ. ನಿಮಗೆ ಲಾಜಿಸ್ಟಿಕ್ಸ್ ಬಗ್ಗೆ ಸಹಾಯ ಬೇಕೇ?",
    "ta": "அதைப் பற்றிய தகவல் என்னிடம் இல்லை. உங்களுக்கு லாஜிஸ்டிக்ஸ் தொடர்பாக உதவி வேண்டுமா?",
    "te": "దాని గురించి నా దగ్గర సమాచారం లేదు. మీకు లాజిస్టిక్స్ విషయంలో సహాయం కావాలా?",
    "ml": "അതിനെക്കുറിച്ച് എന്റെ കൈയിൽ വിവരമില്ല. നിങ്ങൾക്ക് ലോജിസ്റ്റിക്സിൽ സഹായം വേണമോ?",
    "mr": "त्याबद्दल माझ्याकडे माहिती नाही. तुम्हाला लॉजिस्टिक्समध्ये मदत हवी आहे का?",
    "gu": "મારી પાસે તેના વિશે માહિતી નથી. તમને લોજિસ્ટિક્સમાં મદદ જોઈએ છે?",
    "bn": "এই বিষয়ে আমার কাছে তথ্য নেই। আপনার কি লজিস্টিক্সে সাহায্য দরকার?",
    "pa": "ਮੇਰੇ ਕੋਲ ਇਸ ਬਾਰੇ ਜਾਣਕਾਰੀ ਨਹੀਂ ਹੈ। ਕੀ ਤੁਹਾਨੂੰ ਲੌਜਿਸਟਿਕਸ ਵਿੱਚ ਮਦਦ ਚਾਹੀਦੀ ਹੈ?",
    "or": "ସେ ବିଷୟରେ ମୋ ପାଖରେ ସୂଚନା ନାହିଁ। ଆପଣଙ୍କୁ ଲଜିଷ୍ଟିକ୍ସରେ ସାହାଯ୍ୟ ଦରକାର କି?",
    "as": "সেই বিষয়ে মোৰ ওচৰত তথ্য নাই। আপোনাক লজিষ্টিক্সৰ সহায় লাগেনে?",
}


def T(lang, key, **p):
    lang = lang if lang in LANGS else DEFAULT_LANG
    p = dict(p)
    for k in ("dest", "a", "b"):
        if k in p and p[k] in CITIES:
            p[k] = display_city(p[k], lang)
    if "when_ts" in p:
        p["when"] = when_str(p.pop("when_ts"), lang)
    if "st_key" in p:
        p["st"] = T(lang, p.pop("st_key"))
    for k, v in list(p.items()):
        if isinstance(v, float):
            p[k] = n(v)
    if key == "human_offer" and lang in HUMAN_MULTI:
        msg = HUMAN_MULTI[lang]
    elif key == "logistics_only":
        msg = LOGISTICS_ONLY.get(lang, LOGISTICS_ONLY["kn"])
    elif key == "q_when_specific":
        msg = SLOT_SPECIFIC_TIME.get(lang, SLOT_SPECIFIC_TIME["kn"])
    elif lang in SLOT_MULTI and key in SLOT_MULTI[lang]:
        msg = SLOT_MULTI[lang][key]
    elif lang in MULTI and key in MULTI[lang]:
        msg = MULTI[lang][key]
    else:
        entry = M[key]
        if lang == "hi" and len(entry) > 1:
            msg = entry[1]
        elif lang == "kn" and len(entry) > 2:
            msg = entry[2]
        else:
            msg = entry[0] if DEFAULT_LANG == "kn" else (entry[2] if len(entry) > 2 else entry[0])
    return msg.format(**p)


                                                                                       
def now():
    return time.time()


def norm_phone(p):
    d = re.sub(r"\D", "", p or "")
    return d[-10:] if len(d) >= 10 else d


def upsert_user(phone, role=None, lang=None):
    with LOCK:
        u = q1("SELECT * FROM users WHERE phone=?", (phone,))
        if not u:
            ex("INSERT INTO users VALUES(?,?,?)", (phone, role, lang))
        else:
            ex("UPDATE users SET role=?, lang=? WHERE phone=?", (role or u["role"], lang or u["lang"], phone))


def market_rate(dest):
    rows = q("SELECT price*1.0/weight_tons AS r FROM bookings WHERE destination=? AND weight_tons>0 AND price>0 "
             "AND status IN ('confirmed','loaded','in_transit','delivered') AND updated>?", (dest, now() - 30 * 86400))
    return round(statistics.median(r["r"] for r in rows)) if len(rows) >= 2 else None


def get_rate(a, b):
    if a == HUB and market_rate(b):
        return market_rate(b)
    r = q1("SELECT per_tonne FROM rates WHERE origin=? AND dest=?", (a, b)) or q1(
        "SELECT per_tonne FROM rates WHERE origin=? AND dest=?", (b, a))
    return r["per_tonne"] if r else None


def queue_cb(phone, role, kind, booking_id=None, key=None, params=None, delay=0):
    with LOCK:
        if kind == "offer" and q1("SELECT id FROM callbacks WHERE booking_id=? AND role=? AND kind='offer' "
                                  "AND status IN ('pending','dialing','ringing')", (booking_id, role)):
            return
        ex("INSERT INTO callbacks(phone,role,kind,booking_id,msgkey,params,status,next_try,created) VALUES(?,?,?,?,?,?,?,?,?)",
           (phone, role, kind, booking_id, key, json.dumps(params or {}), "pending", now() + delay, now()))
        log("callback queued -> %s (%s) %s" % (phone, role, key or kind))


def void_cbs(booking_id):
    ex("UPDATE callbacks SET status='void' WHERE booking_id=? AND kind='offer' AND status IN ('pending','dialing','ringing')",
       (booking_id,))


def rejected(tid, lid):
    return bool(q1("SELECT 1 FROM rejections WHERE truck_id=? AND load_id=?", (tid, lid)))


def propose(t, l):
    load_weight = float(l["weight_tons"] or 0)
    truck_capacity = float(t["available_capacity_tons"] or 0)
    match_weight = min(load_weight, truck_capacity)
    if match_weight <= 0:
        return None

    if t["expected_price"]:
        price = float(t["expected_price"])
        if truck_capacity > 0 and match_weight < truck_capacity:
            price = price * match_weight / truck_capacity
    elif l["offered_price"]:
        price = float(l["offered_price"])
        if load_weight > 0 and match_weight < load_weight:
            price = price * match_weight / load_weight
    else:
        price = (market_rate(l["destination"]) or 0) * match_weight

    bid = ex("INSERT INTO bookings(truck_id,load_id,destination,weight_tons,price,status,created,updated) "
             "VALUES(?,?,?,?,?,'proposed',?,?)",
             (t["id"], l["id"], l["destination"], match_weight, price, now(), now()))
    ex("UPDATE trucks SET status='held' WHERE id=?", (t["id"],))
    ex("UPDATE loads SET status='held' WHERE id=?", (l["id"],))
    log("MATCH truck#%d <-> load#%d %s %st of %st Rs%s" %
        (t["id"], l["id"], l["destination"], n(match_weight), n(load_weight), n(price)))
    return q1("SELECT * FROM bookings WHERE id=?", (bid,))


def try_match_truck(tid):
    with LOCK:
        t = q1("SELECT * FROM trucks WHERE id=? AND status='available'", (tid,))
        if not t or float(t["available_capacity_tons"] or 0) <= 0:
            return None

        rows = q("SELECT * FROM loads WHERE status='open' AND destination=? "
                 "AND required_by<=? AND required_by>? ORDER BY required_by, weight_tons",
                 (t["destination"], t["available_until"], now()))

        capacity = float(t["available_capacity_tons"] or 0)
        rows.sort(key=lambda l: (
            0 if 0 < float(l["weight_tons"] or 0) <= capacity else 1,
            float(l["weight_tons"] or 0),
            float(l["required_by"] or 0)
        ))

        for l in rows:
            if not ALLOW_SELF_MATCH and l["phone"] == t["phone"]:
                continue
            if rejected(t["id"], l["id"]):
                continue
            return propose(t, l)
    return None


def try_match_load(lid):
    with LOCK:
        l = q1("SELECT * FROM loads WHERE id=? AND status='open'", (lid,))
        if not l:
            return None

        rows = q("SELECT * FROM trucks WHERE status='available' "
                 "AND destination=? AND available_until>=? AND available_until>?",
                 (l["destination"], l["required_by"], now()))

        load_weight = float(l["weight_tons"] or 0)
        rows.sort(key=lambda t: (
            0 if 0 < float(t["available_capacity_tons"] or 0) >= load_weight else 1,
            -float(t["available_capacity_tons"] or 0),
            float(t["available_until"] or 0)
        ))

        for t in rows:
            if float(t["available_capacity_tons"] or 0) <= 0:
                continue
            if not ALLOW_SELF_MATCH and l["phone"] == t["phone"]:
                continue
            if rejected(t["id"], l["id"]):
                continue
            return propose(t, l)
    return None


def parties(b):
    t = q1("SELECT * FROM trucks WHERE id=?", (b["truck_id"],))
    l = q1("SELECT * FROM loads WHERE id=?", (b["load_id"],))
    return t, l


def release_proposed(b, by=None):
    """Free both sides of an unconfirmed offer. 'by' = role that caused it (the other side is told if they had said yes)."""
    with LOCK:
        t, l = parties(b)
        ex("UPDATE bookings SET status='cancelled', updated=? WHERE id=?", (now(), b["id"]))
        if t["status"] == "held":
            ex("UPDATE trucks SET status='available' WHERE id=?", (t["id"],))
        if l["status"] == "held":
            ex("UPDATE loads SET status='open' WHERE id=?", (l["id"],))
        void_cbs(b["id"])
        if by:
            other = "trader" if by == "driver" else "driver"
            if b["trader_ok" if other == "trader" else "driver_ok"]:
                queue_cb((l if other == "trader" else t)["phone"], other, "notice", key="notice_declined",
                         params={"dest": b["destination"]})


def confirm_booking(b, in_call_role):
    t, l = parties(b)
    rest = max(0.0, t["available_capacity_tons"] - b["weight_tons"])
    remaining = max(0.0, l["weight_tons"] - b["weight_tons"])
    ex("UPDATE bookings SET status='confirmed', updated=? WHERE id=?", (now(), b["id"]))
    ex("UPDATE trucks SET available_capacity_tons=?, status=? WHERE id=?",
       (rest, "available" if rest > 0.01 else "booked", t["id"]))

    if remaining > 0.01:
        new_price = None
        if l["offered_price"] is not None and l["weight_tons"] > 0:
            new_price = float(l["offered_price"]) * remaining / float(l["weight_tons"])
        ex("UPDATE loads SET weight_tons=?, offered_price=?, status='open' WHERE id=?",
           (remaining, new_price, l["id"]))
    else:
        ex("UPDATE loads SET status='booked' WHERE id=?", (l["id"],))

    log("BOOKING #%d CONFIRMED %s %st Rs%s; load remaining %st" %
        (b["id"], b["destination"], n(b["weight_tons"]), n(b["price"]), n(remaining)))
    other = "trader" if in_call_role == "driver" else "driver"
    queue_cb((l if other == "trader" else t)["phone"], other, "notice", key="notice_confirmed",
             params={"dest": b["destination"], "w": b["weight_tons"], "price": b["price"]})

    if remaining > 0.01:
        try_match_load(l["id"])


def respond_offer(bid, role, accept, price=None):
    """Returns confirmed | waiting | countered | rejected | gone."""
    with LOCK:
        b = q1("SELECT * FROM bookings WHERE id=?", (bid,))
        if not b or b["status"] != "proposed":
            return "gone"
        t, l = parties(b)
        me = "driver_ok" if role == "driver" else "trader_ok"
        other_role = "trader" if role == "driver" else "driver"
        other = "trader_ok" if role == "driver" else "driver_ok"
        other_phone = (l if other_role == "trader" else t)["phone"]
        if not accept:
            ex("UPDATE bookings SET status='rejected', updated=? WHERE id=?", (now(), bid))
            ex("INSERT INTO rejections VALUES(?,?)", (t["id"], l["id"]))
            if t["status"] == "held":
                ex("UPDATE trucks SET status='available' WHERE id=?", (t["id"],))
            if l["status"] == "held":
                ex("UPDATE loads SET status='open' WHERE id=?", (l["id"],))
            void_cbs(bid)
            if b[other]:
                queue_cb(other_phone, other_role, "notice", key="notice_declined", params={"dest": b["destination"]})
            log("offer #%d rejected by %s" % (bid, role))
            return "rejected"
        if price and abs(price - b["price"]) > 0.5:
            ex("UPDATE bookings SET price=?, price_by=?, %s=1, %s=0, updated=? WHERE id=?" % (me, other),
               (price, role, now(), bid))
            queue_cb(other_phone, other_role, "offer", booking_id=bid)
            log("offer #%d counter Rs%s by %s" % (bid, n(price), role))
            return "countered"
        ex("UPDATE bookings SET %s=1, updated=? WHERE id=?" % me, (now(), bid))
        b = q1("SELECT * FROM bookings WHERE id=?", (bid,))
        if b[other]:
            confirm_booking(b, role)
            return "confirmed"
        queue_cb(other_phone, other_role, "offer", booking_id=bid)
        return "waiting"


def cancel_booking(b, by):
    with LOCK:
        b = q1("SELECT * FROM bookings WHERE id=?", (b["id"],))
        if not b or b["status"] not in ("confirmed", "proposed"):
            return
        t, l = parties(b)
        was = b["status"] == "confirmed"
        if was:
            ex("UPDATE trucks SET available_capacity_tons=MIN(capacity_tons, available_capacity_tons+?), status='available' WHERE id=?",
               (b["weight_tons"], t["id"]))
        elif t["status"] == "held":
            ex("UPDATE trucks SET status='available' WHERE id=?", (t["id"],))
        ex("UPDATE loads SET status=? WHERE id=?", ("cancelled" if by == "trader" else "open", l["id"]))
        ex("UPDATE bookings SET status='cancelled', updated=? WHERE id=?", (now(), b["id"]))
        void_cbs(b["id"])
        other = "driver" if by == "trader" else "trader"
        if was or b["driver_ok" if other == "driver" else "trader_ok"]:
            queue_cb((t if other == "driver" else l)["phone"], other, "notice", key="notice_cancelled",
                     params={"dest": b["destination"]})
        log("booking #%d cancelled by %s" % (b["id"], by))


def cancel_post(role, row):
    with LOCK:
        table = "trucks" if role == "driver" else "loads"
        ex("UPDATE %s SET status='cancelled' WHERE id=?" % table, (row["id"],))
        col = "truck_id" if role == "driver" else "load_id"
        for b in q("SELECT * FROM bookings WHERE %s=? AND status='proposed'" % col, (row["id"],)):
            release_proposed(b, by=role)
        log("%s #%d cancelled" % (table[:-1], row["id"]))


def reschedule(role, row, ts):
    with LOCK:
        if role == "trader":
            ex("UPDATE loads SET required_by=? WHERE id=?", (ts, row["id"]))
            for b in q("SELECT * FROM bookings WHERE load_id=? AND status='proposed'", (row["id"],)):
                release_proposed(b, by="trader")
            for b in q("SELECT * FROM bookings WHERE load_id=? AND status='confirmed'", (row["id"],)):
                t = q1("SELECT * FROM trucks WHERE id=?", (b["truck_id"],))
                queue_cb(t["phone"], "driver", "notice", key="notice_rescheduled",
                         params={"dest": b["destination"], "when_ts": ts})
        else:
            ex("UPDATE trucks SET available_until=? WHERE id=?", (ts, row["id"]))
            for b in q("SELECT * FROM bookings WHERE truck_id=? AND status='proposed'", (row["id"],)):
                release_proposed(b, by="driver")
        log("%s #%d rescheduled to %s" % (role, row["id"], datetime.fromtimestamp(ts, IST).strftime("%d %b %H:%M")))


def my_post(phone, role):
    if role == "driver":
        return q1("SELECT * FROM trucks WHERE phone=? AND status IN ('available','held') ORDER BY id DESC", (phone,))
    return q1("SELECT * FROM loads WHERE phone=? AND status IN ('open','held') ORDER BY id DESC", (phone,))


def my_bookings(phone, role, statuses):
    j, col = ("trucks t ON t.id=b.truck_id", "t.phone") if role == "driver" else ("loads l ON l.id=b.load_id", "l.phone")
    return q("SELECT b.* FROM bookings b JOIN %s WHERE %s=? AND b.status IN (%s) ORDER BY b.id DESC"
             % (j, col, ",".join("'%s'" % s for s in statuses)), (phone,))


def pending_offer(phone, role):
    for b in my_bookings(phone, role, ["proposed"]):
        if not b["driver_ok" if role == "driver" else "trader_ok"]:
            return b
    return None


def set_trip_status(b, st):
    with LOCK:
        ex("UPDATE bookings SET status=?, updated=? WHERE id=?", (st, now(), b["id"]))
        t, l = parties(b)
        if st == "delivered":
            ex("UPDATE loads SET status='done' WHERE id=?", (l["id"],))
            if t["status"] == "booked":
                ex("UPDATE trucks SET status='done' WHERE id=?", (t["id"],))
        queue_cb(l["phone"], "trader", "notice", key="notice_status", params={"dest": b["destination"], "st_key": "st_" + st})
        log("booking #%d -> %s" % (b["id"], st))


def notify_new_offer(b):
    """Used by the background sweep: ring whoever has been waiting longer."""
    t, l = parties(b)
    first = "driver" if t["created"] <= l["created"] else "trader"
    queue_cb((t if first == "driver" else l)["phone"], first, "offer", booking_id=b["id"])


def sweep():
    with LOCK:
        for t in q("SELECT * FROM trucks WHERE status='available' AND available_until<?", (now(),)):
            ex("UPDATE trucks SET status='expired' WHERE id=?", (t["id"],))
            queue_cb(t["phone"], "driver", "notice", key="notice_expired", params={"dest": t["destination"]})
        for l in q("SELECT * FROM loads WHERE status='open' AND required_by<?", (now(),)):
            ex("UPDATE loads SET status='expired' WHERE id=?", (l["id"],))
            queue_cb(l["phone"], "trader", "notice", key="notice_expired", params={"dest": l["destination"]})
        for b in q("SELECT * FROM bookings WHERE status='proposed' AND updated<?", (now() - OFFER_TTL,)):
            release_proposed(b)
        for l in q("SELECT id FROM loads WHERE status='open'"):
            b = try_match_load(l["id"])
            if b:
                notify_new_offer(b)


                                                                                  
def merge_wavs(chunks):
    if len(chunks) == 1:
        return chunks[0]
    out = io.BytesIO()
    with wave.open(io.BytesIO(chunks[0])) as w0:
        params = w0.getparams()
    with wave.open(out, "wb") as w:
        w.setparams(params)
        for c in chunks:
            with wave.open(io.BytesIO(c)) as wr:
                w.writeframes(wr.readframes(wr.getnframes()))
    return out.getvalue()


def tts_wav(text, lang):
    if not client:
        return None
    try:
        kw = dict(text=text[:2500], speaker=VOICE, model=TTS_MODEL)
        try:
            r = client.text_to_speech.convert(target_language_code=LANG_CODE[lang], **kw)
        except TypeError:
            r = client.text_to_speech.convert(language_code=LANG_CODE[lang], **kw)
        return merge_wavs([base64.b64decode(a) for a in r.audios])
    except Exception as e:
        print("TTS error:", e)
        return None


def stt(audio, filename, mime):
    """Returns (text, 'en'|'hi'|'kn'|None)."""
    if not client:
        raise RuntimeError("Speech needs SARVAM_API_KEY and `pip install sarvamai`. Type instead.")
    r = client.speech_to_text.transcribe(file=(filename, audio, mime), model=STT_MODEL, language_code="unknown")
    lc = (getattr(r, "language_code", None) or "")[:2]
    return (getattr(r, "transcript", "") or "").strip(), (lc if lc in LANGS else None)


AUDIO = {}                                                  


def store_audio(wav):
    for k in [k for k, (_, e) in AUDIO.items() if e < now()]:
        AUDIO.pop(k, None)
    aid = uuid.uuid4().hex
    AUDIO[aid] = (wav, now() + 600)
    return "%s/audio/%s.wav" % (PUBLIC_URL, aid)


                                                                                
SLOTS = {"truck": ["dest", "cap", "until", "price"], "load": ["dest", "weight", "by"]}
QKEY = {("truck", "dest"): "q_dest_truck", ("truck", "cap"): "q_cap", ("truck", "until"): "q_hours",
        ("truck", "price"): "q_price", ("load", "dest"): "q_dest_load", ("load", "weight"): "q_weight",
        ("load", "by"): "q_when"}
SESSIONS = {}


class Session:
    def __init__(self, phone, lang, role=None, sid=None):
        self.id = sid or uuid.uuid4().hex[:12]
        self.phone, self.lang, self.role = phone, lang, role
        self.state, self.flow, self.slots = "ROOT", None, {}
        self.fails, self.slot_fails, self.prev, self.booking_id, self.target = 0, 0, None, None, None
        self.dest_options = []
        self.last_prompt, self.ended, self.handoff, self.touched = "", False, False, now()


def ask(s, state, text):
    s.state, s.last_prompt = state, text
    return text


def idle(s, text=""):
    """Finish a task and leave the line open for one more request."""
    s.state, s.flow, s.slots, s.fails = "ROOT", None, {}, 0
    s.last_prompt = T(s.lang, "anything_else")
    return (text + " " + s.last_prompt).strip()


def fail(s):
    s.fails += 1

                                                                        
                                                                      
                                                                        
    if s.state == "SLOT":
        prompt = s.last_prompt or T(s.lang, QKEY[(s.flow, next_slot(s))])
        s.fails = 0
        return T(s.lang, "unclear") + " " + prompt

    if s.state == "HUMAN_OFFER":
        if s.fails >= 3:
            s.handoff = s.ended = True
            return T(s.lang, "human_go")
        return T(s.lang, "unclear") + T(s.lang, "human_offer")
    if s.fails >= 2:
        s.prev, s.fails = s.state, 1
        return ask(s, "HUMAN_OFFER", T(s.lang, "human_offer"))
    return T(s.lang, "unclear") + (s.last_prompt or T(s.lang, "ask_role"))


def present_offer(s, b):
    t, l = parties(b)
    s.booking_id = b["id"]
    pre = T(s.lang, "counter_pre", price=b["price"]) if b["price_by"] and b["price_by"] != s.role else ""
    partial = float(b["weight_tons"]) + 0.01 < float(l["weight_tons"])
    if s.role == "driver":
        if partial:
            body = T(s.lang, "offer_driver_partial",
                     total=l["weight_tons"], cap=t["available_capacity_tons"],
                     w=b["weight_tons"], dest=b["destination"],
                     when_ts=l["required_by"], price=b["price"])
        else:
            body = T(s.lang, "offer_driver", w=b["weight_tons"], dest=b["destination"],
                     when_ts=l["required_by"], price=b["price"])
    else:
        if partial:
            body = T(s.lang, "offer_trader_partial",
                     total=l["weight_tons"], cap=t["available_capacity_tons"],
                     w=b["weight_tons"], dest=b["destination"], price=b["price"])
        else:
            body = T(s.lang, "offer_trader", cap=t["available_capacity_tons"],
                     dest=b["destination"], price=b["price"])
    return ask(s, "OFFER", pre + body)


def begin_call(phone, cb_id=None, lang=None, sid=None):
    u = q1("SELECT * FROM users WHERE phone=?", (phone,))
    s = Session(phone, lang or (u and u["lang"]) or DEFAULT_LANG, u and u["role"], sid)
    cb = q1("SELECT * FROM callbacks WHERE id=?", (cb_id,)) if cb_id else None
    if cb:
        s.role = cb["role"]
        ex("UPDATE callbacks SET status='answered' WHERE id=?", (cb["id"],))
        pre = T(s.lang, "cb_greet") + " "
        if cb["kind"] == "offer":
            b = q1("SELECT * FROM bookings WHERE id=?", (cb["booking_id"],))
            mine = "driver_ok" if s.role == "driver" else "trader_ok"
            if b and b["status"] == "proposed" and not b[mine]:
                reply = pre + present_offer(s, b)
            else:
                reply = idle(s, pre + T(s.lang, "offer_gone"))
        else:
            reply = idle(s, pre + T(s.lang, cb["msgkey"], **json.loads(cb["params"] or "{}")))
    else:
        reply = ask(s, "ROOT", T(s.lang, "greet"))
    SESSIONS[s.id] = s
    return s, reply


def next_slot(s):
    return next((k for k in SLOTS[s.flow] if k not in s.slots), None)


def fill(s, t):
    nw = now()
    e = extract(t, nw)
    sl, asked = s.slots, next_slot(s)
    tk, wk = ("cap", "until") if s.flow == "truck" else ("weight", "by")
    prog = False
    if e["dest"]:
        sl["dest"], prog = e["dest"], True
    if e["tons"] and 0 < e["tons"] <= 60:
        sl[tk], prog = e["tons"], True
    if e["ts"]:
        sl[wk], prog = e["ts"], True
    if e["price"] and 100 <= e["price"] <= 2000000:
        sl["price"], prog = e["price"], True
    if not prog and asked:
        m = re.search(NUM, t)
        if m:
            x = val(m.group(1))
            if asked in ("cap", "weight") and 0 < x <= 60:
                sl[asked], prog = x, True
            elif asked in ("until", "by") and 0 < x <= 240:
                sl[asked], prog = nw + x * 3600, True
            elif asked == "price" and x >= 100:
                sl["price"], prog = x, True
    return prog


def start_flow(s, role, t, prefix=""):
    s.role, s.flow, s.slots = role, ("truck" if role == "driver" else "load"), {}
    fill(s, t)
    return prefix + continue_flow(s, t, starting=True, filled=True)


def continue_flow(s, t, starting=False, filled=False):
    if not filled and not starting:
        if not fill(s, t):
            if next_slot(s) == "by" and any(has_any(t, ws) for ws, _ in PERIODS):
                return ask(s, "SLOT", T(s.lang, "q_when_specific"))
            if next_slot(s) == "dest":
                s.slot_fails += 1
                if s.slot_fails == 1:
                    return T(s.lang, "dest_unrecognized") + T(s.lang, QKEY[(s.flow, "dest")])
                s.state = "DEST_OPTIONS"
                s.fails = 0
                msg = destination_list_text(s)
                return ask(s, "DEST_OPTIONS", msg)
            return fail(s)
    s.fails = 0
    s.slot_fails = 0
    ns = next_slot(s)
    if ns:
        prompt = T(s.lang, QKEY[(s.flow, ns)])
        return ask(s, "SLOT", T(s.lang, "got_it") + prompt)
    return finish_flow(s)


def finish_flow(s):
    sl, nw, L = s.slots, now(), s.lang
    with LOCK:
        upsert_user(s.phone, s.role, s.lang)
        if s.flow == "truck":
            tid = ex("INSERT INTO trucks(phone,destination,capacity_tons,available_capacity_tons,available_until,"
                     "expected_price,status,created) VALUES(?,?,?,?,?,?,?,?)",
                     (s.phone, sl["dest"], TRUCK_CAPACITY_T, min(sl["cap"], TRUCK_CAPACITY_T), sl["until"], sl["price"],
                      "available", nw))
            log("truck#%d registered %s %st until %s" % (tid, sl["dest"], n(sl["cap"]),
                                                         datetime.fromtimestamp(sl["until"], IST).strftime("%H:%M")))
            b = try_match_truck(tid)
            out = T(L, "sum_truck", cap=sl["cap"], dest=sl["dest"], hours=dur_str((sl["until"] - nw) / 3600, L), price=sl["price"])
            nomatch = T(L, "no_match_truck", dest=sl["dest"])
        else:
            lid = ex("INSERT INTO loads(phone,destination,weight_tons,required_by,offered_price,status,created) "
                     "VALUES(?,?,?,?,?,'open',?)", (s.phone, sl["dest"], sl["weight"], sl["by"], sl.get("price"), nw))
            log("load#%d posted %s %st" % (lid, sl["dest"], n(sl["weight"])))
            b = try_match_load(lid)
            out = T(L, "sum_load", w=sl["weight"], dest=sl["dest"], when_ts=sl["by"])
            nomatch = T(L, "no_match_load", dest=sl["dest"])
    if b:
        s.flow, s.slots = None, {}
        return out + " " + present_offer(s, b)
    return idle(s, out + " " + nomatch)


def check_role(t, s):
    if has_re(t, [r"my (truck|lorry|gaadi)", r"मेरे? (ट्रक|गाड़ी)", r"ನನ್ನ (ಟ್ರಕ್|ಲಾರಿ)"]):
        return "driver"
    if has_any(t, TRUCK_W):
        return "trader"
    if has_any(t, LOAD_W):
        return "driver"
    return s.role


def do_check(s, t):
    role = check_role(t, s)
    if not role:
        return fail(s)
    s.role = role
    b = pending_offer(s.phone, role)
    if b:
        return present_offer(s, b)
    post = my_post(s.phone, role)
    if not post:
        return start_flow(s, role, t, T(s.lang, "none_" + role))
    if post["status"] == "held":
        return idle(s, T(s.lang, "accepted_wait"))
    b = try_match_truck(post["id"]) if role == "driver" else try_match_load(post["id"])
    if b:
        return present_offer(s, b)
    return idle(s, T(s.lang, "no_match_" + ("truck" if role == "driver" else "load"), dest=post["destination"]))


def handle_offer(s, t):
    b = q1("SELECT * FROM bookings WHERE id=?", (s.booking_id,))
    if not b or b["status"] != "proposed":
        return idle(s, T(s.lang, "offer_gone"))
    e = extract(t, now())
    price = e["price"] if (e["price"] and e["price"] >= 100) else None
    if price is None and not has_any(t, YES) and not has_any(t, NO):
        m = re.search(r"\d+(?:\.\d+)?", t)
        if m and float(m.group()) >= 100:
            price = float(m.group())
    if price and abs(price - b["price"]) > 0.5:
        res = respond_offer(b["id"], s.role, True, price)
    elif has_any(t, NO):
        res = respond_offer(b["id"], s.role, False)
    elif has_any(t, YES) or price:
        res = respond_offer(b["id"], s.role, True)
    else:
        return fail(s)
    s.fails = 0
    b = q1("SELECT * FROM bookings WHERE id=?", (b["id"],))
    if res == "confirmed":
        key = "confirmed_driver" if s.role == "driver" else "confirmed_trader"
        return idle(s, T(s.lang, key, dest=b["destination"], w=b["weight_tons"], price=b["price"]))
    if res == "waiting":
        return idle(s, T(s.lang, "accepted_wait"))
    if res == "countered":
        return idle(s, T(s.lang, "counter_sent", price=b["price"]))
    if res == "rejected":
        return idle(s, T(s.lang, "rejected_keep"))
    return idle(s, T(s.lang, "offer_gone"))


def need_role(s, t):
    return detect_role(t) or s.role


def start_cancel(s, t):
    role = need_role(s, t)
    if not role:
        return fail(s)
    s.role = role
    bk = my_bookings(s.phone, role, ["confirmed", "proposed"])
    if bk:
        tgt, w, what, dest = ("booking", bk[0]), bk[0]["weight_tons"], "what_booking", bk[0]["destination"]
    else:
        post = my_post(s.phone, role)
        if post:
            w = post["weight_tons"] if role == "trader" else post["available_capacity_tons"]
            tgt, what, dest = ("post", post), "what_load" if role == "trader" else "what_truck", post["destination"]
        elif my_bookings(s.phone, role, ["loaded", "in_transit"]):
            return idle(s, T(s.lang, "cancel_late"))
        else:
            return idle(s, T(s.lang, "cancel_none"))
    s.target = tgt
    return ask(s, "CANCEL_CONFIRM", T(s.lang, "cancel_ask", dest=dest, w=w, what=T(s.lang, what)))


def handle_cancel(s, t):
    if has_any(t, NO):
        return idle(s, T(s.lang, "cancel_keep"))
    if not has_any(t, YES):
        return fail(s)
    kind, row = s.target
    if kind == "booking":
        cancel_booking(row, s.role)
        return idle(s, T(s.lang, "cancel_done_" + s.role))
    cancel_post(s.role, row)
    return idle(s, T(s.lang, "cancel_done_" + ("trader" if s.role == "trader" else "truck")))


def start_resched(s, t):
    role = need_role(s, t)
    row = my_post(s.phone, role) if role else None
    if not row and role:
        bk = my_bookings(s.phone, role, ["confirmed"])
        if bk and role == "trader":
            row = q1("SELECT * FROM loads WHERE id=?", (bk[0]["load_id"],))
    if not row:
        if role:
            return start_flow(s, role, t)
        return fail(s)
    s.role, s.target = role, row
    ts = parse_when(t, now())
    if ts:
        return apply_resched(s, ts)
    return ask(s, "RESCHED_WHEN", T(s.lang, "resched_q"))


def apply_resched(s, ts):
    reschedule(s.role, s.target, ts)
    return idle(s, T(s.lang, "resched_" + ("load" if s.role == "trader" else "truck"), when_ts=ts))


def handle_resched(s, t):
    ts = parse_when(t, now())
    if not ts:
        m = re.search(NUM, t)
        ts = now() + val(m.group(1)) * 3600 if m else None
    return apply_resched(s, ts) if ts else fail(s)


def do_status(s, st):
    bk = my_bookings(s.phone, "driver", ["confirmed", "loaded", "in_transit"])
    if not bk:
        return idle(s, T(s.lang, "status_none"))
    set_trip_status(bk[0], st)
    s.role = "driver"
    return idle(s, T(s.lang, "status_" + st))


def start_rate(s, t):
    cs = find_cities(t)
    if len(cs) >= 2 and cs[0] != cs[-1]:
        return say_rate(s, cs[0], cs[-1])
    if cs:
        return say_rate(s, HUB, cs[0])
    return ask(s, "RATE_DEST", T(s.lang, "rate_which"))


def say_rate(s, a, b):
    r = get_rate(a, b)
    if not r:
        return idle(s, T(s.lang, "rate_unknown", a=a, b=b))
    s.slots = {"dest": b}
    return ask(s, "RATE_FOLLOW", T(s.lang, "rate_say", a=a, b=b, rate=r, full=r * TRUCK_CAPACITY_T))


OFF_TOPIC_PATTERNS = [
    r"^who\s+(is|was|are|were)\b",
    r"^what\s+(is|are|was|were)\b",
    r"^why\s+",
    r"^tell\s+me\s+about\b",
    r"^weather\b",
]


def is_off_topic(t):
    low = (t or "").strip().lower()
    if not low:
        return False
    logistics_words = [
        "load", "truck", "lorry", "driver", "goods", "ton", "tons", "route",
        "destination", "delivery", "fare", "price", "rate", "booking",
        "pickup", "drop", "cargo", "logistics", "ಟ್ರಕ್", "ಲೋಡ್", "ಚಾಲಕ",
        "ಮಾಲು", "ಸರಕು", "ಭಾಡ", "ಲಾಜಿಸ್ಟಿಕ್ಸ್", "लोड", "ट्रक", "ड्राइवर",
        "माल", "भाड़ा", "डिलीवरी", "ट्रिप",
    ]
    if any(w in low for w in logistics_words):
        return False
    return any(re.search(p, low) for p in OFF_TOPIC_PATTERNS)


def step(s, text):
    t = norm(text)
    s.touched = now()
    if not t:
        return fail(s)
    st = s.state
    if is_off_topic(t):
        return T(s.lang, "logistics_only")
    if st == "HUMAN_OFFER":
        if has_any(t, YES):
            s.handoff = s.ended = True
            return T(s.lang, "human_go")
        if has_any(t, NO):
            s.state, s.fails = s.prev or "ROOT", 0
            return s.last_prompt if s.prev else T(s.lang, "ask_role")
        return fail(s)
    if has_any(t, HUMAN):
        s.handoff = s.ended = True
        return T(s.lang, "human_go")
    if st == "OFFER":
        return handle_offer(s, t)
    if st == "CANCEL_CONFIRM":
        return handle_cancel(s, t)
    if st == "RESCHED_WHEN":
        return handle_resched(s, t)
    if st == "RATE_DEST":
        cs = find_cities(t)
        return say_rate(s, HUB, cs[-1]) if cs else fail(s)
    if st == "RATE_FOLLOW":
        if has_any(t, YES):
            return start_flow(s, "driver", "", "") if not s.slots.get("dest") else _rate_to_flow(s)
        return idle(s) if has_any(t, NO) else fail(s)
    if st == "DEST_OPTIONS":
        nums = re.findall(r"\d+", t)
        if not nums:
            return T(s.lang, "dest_choose") + "\n" + "\n".join("%d. %s" % (i + 1, display_city(d, s.lang)) for i, d in enumerate(s.dest_options))
        first = int(nums[0])
        if 1 <= first <= len(s.dest_options):
            s.slots["dest"] = s.dest_options[first - 1]
            s.dest_options = []
            s.slot_fails = 0
            return continue_flow(s, "", starting=False, filled=True)
        return T(s.lang, "dest_bad_number") + "\n" + "\n".join("%d. %s" % (i + 1, display_city(d, s.lang)) for i, d in enumerate(s.dest_options))
    if st == "SLOT":
        if has_re(t, CANCEL):
            return idle(s, T(s.lang, "cancel_keep"))
        return continue_flow(s, t)
                                  
    for key, pats in (("delivered", DELIVERED), ("loaded", LOADED), ("in_transit", TRANSIT)):
        if has_re(t, pats):
            return do_status(s, key)
    if has_re(t, RESCHED):
        return start_resched(s, t)
    if has_re(t, CANCEL):
        return start_cancel(s, t)
    if has_any(t, RATE):
        return start_rate(s, t)
    if has_re(t, CHECK):
        return do_check(s, t)
    role = detect_role(t)
    e = extract(t, now())
    if role or (s.role and (e["dest"] or e["tons"] or e["price"])):
        return start_flow(s, role or s.role, t)
    if has_any(t, NO + BYE) and not e["dest"]:
        s.ended = True
        return T(s.lang, "bye")
    return fail(s)


def _rate_to_flow(s):
    dest = s.slots["dest"]
    s.role, s.flow, s.slots = "driver", "truck", {"dest": dest}
    upsert_user(s.phone, "driver", s.lang)
    return continue_flow(s, "", starting=True, filled=True)


def run_turn(s, text, lang_hint=None):
    L = lang_hint or detect_lang(text or "")
    if L:
        s.lang = L
    return step(s, text)


def purge_sessions():
    for k in [k for k, v in SESSIONS.items() if now() - v.touched > 3600]:
        SESSIONS.pop(k, None)


                                                                                     
def exotel_ws_url():
    if EXO_STREAM_URL:
        return EXO_STREAM_URL
    if PUBLIC_URL:
        return PUBLIC_URL.replace("https://", "wss://", 1).replace("http://", "ws://", 1) + "/exotel/ws"
    return ""


def exotel_resolver_url():
    return PUBLIC_URL.rstrip("/") + "/exotel/resolve" if PUBLIC_URL else ""


def health_payload():
    return {
        "status": "ok",
        "service": "amargol-load-help",
        "mode": EXOTEL_MODE,
        "provider": PROVIDER,
        "exophone": EXO_EXOPHONE,
        "websocket": exotel_ws_url(),
        "resolver": exotel_resolver_url(),
        "speech": bool(client),
        "schema_version": SCHEMA_VERSION,
    }




# VERCEL / FASTAPI TRANSPORT
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import JSONResponse
import asyncio

app = FastAPI(title="Amargol Load Help", docs_url=None, redoc_url=None)
tlog("BOOT", "FastAPI backend initialized; frontend=DISABLED outbound_exotel_rest=DISABLED")
tlog("BOOT", "Provider=%s mode=%s exophone=%s sarvam=%s" % (PROVIDER, EXOTEL_MODE, EXO_EXOPHONE, "READY" if client else "MISSING"))
tlog("BOOT", "Configured WSS=%s" % (EXO_STREAM_URL or "AUTO_FROM_DEPLOYMENT_HOST"))

@app.get("/")
async def root():
    return JSONResponse({"service":"amargol-load-help","mode":"inbound_voicebot","frontend":False,"outbound_exotel_rest":False,"websocket":"/exotel/ws"})

@app.get("/healthz")
async def healthz(request: Request):
    payload = health_payload()
    public = _request_public_url(request)
    if not payload["websocket"]:
        payload["websocket"] = public.replace("https://", "wss://", 1).replace("http://", "ws://", 1) + "/exotel/ws"
    if not payload["resolver"]:
        payload["resolver"] = public + "/exotel/resolve"
    return JSONResponse(payload)

def _request_public_url(request: Request):
    if PUBLIC_URL:
        return PUBLIC_URL.rstrip("/")
    proto = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("x-forwarded-host", request.headers.get("host", ""))
    return ("%s://%s" % ("https" if proto == "https" else "http", host)).rstrip("/")

@app.get("/exotel/config")
async def exotel_config(request: Request):
    public = _request_public_url(request)
    ws_url = EXO_STREAM_URL or public.replace("https://", "wss://", 1).replace("http://", "ws://", 1) + "/exotel/ws"
    return JSONResponse({
        "provider": "exotel",
        "mode": "inbound_voicebot",
        "exophone": EXO_CALLER,
        "websocket": ws_url,
        "resolver": public + "/exotel/resolve",
        "sarvam_configured": bool(client),
        "outbound_rest": False,
        "frontend": False
    })

@app.get("/exotel/resolve")
async def exotel_resolve(request: Request):
    public = _request_public_url(request)
    ws_url = EXO_STREAM_URL or public.replace("https://", "wss://", 1).replace("http://", "ws://", 1) + "/exotel/ws"
    tlog("EXOTEL", "resolver -> %s" % ws_url)
    return JSONResponse({"url": ws_url})

def _pcm_rms(raw):
    return pcm_rms(raw)

async def _send_tts(websocket: WebSocket, stream_sid: str, text: str, lang: str):
    wav = await asyncio.to_thread(tts_wav, text, lang)
    if not wav:
        tlog("TTS", "no audio generated")
        return 0
    pcm = await asyncio.to_thread(wav_to_pcm16k_or_8k, wav, 8000)
    sent = 0
    for i in range(0, len(pcm), 3200):
        chunk = pcm[i:i+3200]
        await websocket.send_json({
            "event":"media",
            "stream_sid":stream_sid,
            "media":{"payload":base64.b64encode(chunk).decode()}
        })
        sent += len(chunk)
    await websocket.send_json({
        "event":"mark",
        "stream_sid":stream_sid,
        "mark":{"name":"turn-end"}
    })
    return sent

@app.websocket("/exotel/ws")
async def exotel_ws(websocket: WebSocket):
    await websocket.accept()
    stream_sid = None
    call_sid = None
    s = None
    pcm_buf = bytearray()
    speech_seen = False
    silence_ms = 0
    last_media = time.time()
    rx_bytes = 0
    tx_bytes = 0
    media_count = 0
    started_at = time.time()
    tlog("WS", "Exotel AgentStream connection accepted")

    async def process_audio(raw):
        nonlocal tx_bytes
        if not raw or not s or not stream_sid:
            return False
        tlog("AUDIO", "utterance captured bytes=%d duration=%.2fs" % (len(raw), len(raw)/16000.0))
        try:
            wav = wav_from_pcm(raw, 8000)
            text, heard_lang = await asyncio.to_thread(stt, wav, "exotel.wav", "audio/wav")
            tlog("STT", "transcript=%r lang=%s" % (text, heard_lang or "unknown"))
            if not text:
                tlog("AI", "empty transcript; waiting for next utterance")
                return False
            reply = await asyncio.to_thread(run_turn, s, text, heard_lang)
            tlog("AI", "reply=%r state=%s flow=%s ended=%s" % (reply, s.state, s.flow, s.ended))
            sent = await _send_tts(websocket, stream_sid, reply, s.lang)
            tx_bytes += sent
            tlog("TTS", "sent pcm_bytes=%d" % sent)
            return s.ended
        except Exception as e_:
            tlog("ERROR", "STT/AI/TTS failure: %s" % e_)
            try:
                fallback = T(s.lang, "dont_know") if s else "I am unable to process that right now."
                sent = await _send_tts(websocket, stream_sid, fallback, s.lang if s else DEFAULT_LANG)
                tx_bytes += sent
            except Exception as e2:
                tlog("ERROR", "fallback audio failed: %s" % e2)
            return False

    try:
        while True:
            try:
                msg = await asyncio.wait_for(websocket.receive_text(), timeout=1.0)
            except asyncio.TimeoutError:
                if speech_seen and pcm_buf and time.time()-last_media > 0.75:
                    raw=bytes(pcm_buf); pcm_buf.clear(); speech_seen=False; silence_ms=0
                    if await process_audio(raw):
                        break
                continue
            except WebSocketDisconnect:
                break

            try:
                ev=json.loads(msg)
            except Exception:
                tlog("WS", "ignored non-JSON frame")
                continue
            event=ev.get("event")

            if event == "connected":
                tlog("EXOTEL", "connected event received")
                continue

            if event == "start":
                start=ev.get("start") or {}
                stream_sid=ev.get("stream_sid") or start.get("stream_sid")
                call_sid=start.get("call_sid") or stream_sid or uuid.uuid4().hex
                phone=norm_phone(start.get("from", "")) or "0000000000"
                exophone=start.get("to", "") or ""
                direction=start.get("direction", "incoming") or "incoming"
                custom=start.get("custom_parameters") or {}
                forced_lang=custom.get("lang") if custom.get("lang") in LANGS else None
                tlog("EXOTEL", "START call_sid=%s stream_sid=%s from=%s to=%s direction=%s" % (call_sid,stream_sid,phone,exophone,direction))
                s,reply=await asyncio.to_thread(begin_call,phone,None,forced_lang,call_sid)
                tlog("SESSION", "created id=%s language=%s" % (s.id,s.lang))
                sent=await _send_tts(websocket,stream_sid,reply,s.lang)
                tx_bytes+=sent
                tlog("TTS", "greeting sent pcm_bytes=%d" % sent)
                continue

            if event == "media":
                payload=(ev.get("media") or {}).get("payload")
                if not payload: continue
                try: chunk=base64.b64decode(payload)
                except Exception:
                    tlog("WS", "invalid base64 media payload"); continue
                pcm_buf.extend(chunk); rx_bytes+=len(chunk); media_count+=1; last_media=time.time()
                rms=pcm_rms(chunk)
                if rms > 450:
                    speech_seen=True; silence_ms=0
                elif speech_seen:
                    silence_ms+=100
                if media_count % 20 == 0:
                    tlog("MEDIA", "rx_chunks=%d rx_kb=%.1f buffer_kb=%.1f rms=%.0f" % (media_count,rx_bytes/1024, len(pcm_buf)/1024,rms))
                if speech_seen and silence_ms >= 700:
                    raw=bytes(pcm_buf); pcm_buf.clear(); speech_seen=False; silence_ms=0
                    if await process_audio(raw):
                        tlog("CALL", "conversation completed"); break
                continue

            if event == "dtmf":
                digit=str((ev.get("dtmf") or {}).get("digit") or "")
                tlog("EXOTEL", "DTMF digit=%r" % digit)
                if digit and s:
                    reply=await asyncio.to_thread(run_turn,s,digit,s.lang)
                    sent=await _send_tts(websocket,stream_sid,reply,s.lang); tx_bytes+=sent
                    if s.ended: break
                continue

            if event == "mark":
                tlog("EXOTEL", "mark=%r" % ((ev.get("mark") or {}).get("name",""))); continue
            if event == "clear":
                tlog("EXOTEL", "clear event received"); continue
            if event == "stop":
                stop=ev.get("stop") or {}
                tlog("EXOTEL", "STOP call_sid=%s reason=%s" % (stop.get("call_sid") or call_sid or "-", stop.get("reason") or "unknown"))
                break
            tlog("EXOTEL", "unhandled event=%r" % event)
    finally:
        tlog("WS", "session closed call_sid=%s duration=%.1fs rx_kb=%.1f tx_kb=%.1f media=%d" % (call_sid or "-",time.time()-started_at,rx_bytes/1024,tx_bytes/1024,media_count))

