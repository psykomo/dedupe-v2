import random
import time
from typing import List, Dict, Any
import datetime
import typer
from sqlalchemy import text, inspect
from faker import Faker
from rich.progress import track
from dedupe_gemini.db import get_engine

fake = Faker('id_ID')

def generate_nik(dob: datetime.date, gender: str) -> str:
    """
    Generate a valid-looking Indonesian NIK (Nomor Induk Kependudukan).
    Format: PPKKCCDDMMYYSSSS
    PP: Province (11-92)
    KK: City/Regency (01-99)
    CC: District (01-99)
    DD: Date (01-31). For female, +40.
    MM: Month (01-12)
    YY: Year (00-99)
    SSSS: Serial (0001-9999)
    """
    prov = random.randint(11, 92)
    city = random.randint(1, 99)
    dist = random.randint(1, 99)
    
    day = dob.day
    if gender == 'P':
        day += 40
        
    month = dob.month
    year = dob.year % 100
    
    serial = random.randint(1, 9999)
    
    return f"{prov:02d}{city:02d}{dist:02d}{day:02d}{month:02d}{year:02d}{serial:04d}"

def generate_identity_keys(index: int) -> Dict[str, str]:
    """
    Generate unique NOMOR_INDUK and matching ID_UPT based on index.
    Format: UUUYYYYMMDDSSSS
    UUU: ID UPT (3 digits)
    YYYYMMDD: Registration Date
    SSSS: Sequence (4 digits)
    """
    # Base date for simulation (e.g., starting from 2010)
    start_date = datetime.date(2010, 1, 1)
    
    # We allow up to 9999 records per day to fit in SSSS
    records_per_day = 5000 # Safe margin
    
    day_offset = index // records_per_day
    seq = (index % records_per_day) + 1
    
    reg_date = start_date + datetime.timedelta(days=day_offset)
    
    # Generate random UPT code (e.g., 001 to 099)
    # Using a subset of UPTs to make it realistic (simulating 50 UPTs)
    upt_int = random.randint(1, 50)
    upt_code = f"{upt_int:03d}"
    
    nomor_induk = f"{upt_code}{reg_date.strftime('%Y%m%d')}{seq:04d}"
    
    return {
        "NOMOR_INDUK": nomor_induk,
        "ID_UPT": upt_code
    }

def generate_base_record(index: int) -> Dict[str, Any]:
    gender = random.choice(['L', 'P'])
    name = fake.name_male() if gender == 'L' else fake.name_female()
    dob = fake.date_of_birth(minimum_age=17, maximum_age=80)
    nik = generate_nik(dob, gender)
    
    keys = generate_identity_keys(index)
    
    return {
        'NOMOR_INDUK': keys['NOMOR_INDUK'],
        'ID_JENIS_SUKU': str(random.randint(1, 100)),
        'ID_JENIS_SUKU_LAIN': None,
        'ID_JENIS_RAMBUT': str(random.randint(1, 10)),
        'ID_JENIS_MUKA': str(random.randint(1, 10)),
        'ID_JENIS_PENDIDIKAN': str(random.randint(1, 10)),
        'ID_JENIS_TANGAN': str(random.randint(1, 5)),
        'ID_JENIS_AGAMA': str(random.randint(1, 6)),
        'ID_JENIS_AGAMA_LAIN': None,
        'ID_JENIS_PEKERJAAN': str(random.randint(1, 20)),
        'ID_JENIS_PEKERJAAN_LAIN': None,
        'ID_USER': str(random.randint(1, 100)),
        'ID_BENTUK_MATA': str(random.randint(1, 10)),
        'ID_WARNA_MATA': str(random.randint(1, 10)),
        'ID_JENIS_KEAHLIAN_1': str(random.randint(1, 50)),
        'ID_JENIS_KEAHLIAN_1_LAIN': None,
        'ID_JENIS_KEAHLIAN_2': str(random.randint(1, 50)),
        'ID_JENIS_KEAHLIAN_2_LAIN': None,
        'ID_JENIS_HIDUNG': str(random.randint(1, 10)),
        'ID_JENIS_LEVEL_1': str(random.randint(1, 5)),
        'ID_JENIS_MULUT': str(random.randint(1, 10)),
        'ID_JENIS_LEVEL_2': str(random.randint(1, 5)),
        'ID_JENIS_WARGANEGARA': 'WNI' if random.random() > 0.1 else 'WNA',
        'ID_NEGARA_ASING': None,
        'ID_PROPINSI': str(random.randint(1, 34)),
        'ID_PROPINSI_LAIN': None,
        'ID_JENIS_STATUS_PERKAWINAN': str(random.randint(1, 4)),
        'ID_JENIS_KELAMIN': gender,
        'ID_JENIS_KAKI': str(random.randint(1, 5)),
        'ID_TEMPAT_LAHIR': fake.city(),
        'ID_TEMPAT_LAHIR_LAIN': None,
        'ID_KOTA': fake.city(),
        'ID_KOTA_LAIN': None,
        'ID_TEMPAT_ASAL': fake.city(),
        'ID_TEMPAT_ASAL_LAIN': None,
        'RESIDIVIS': '1' if random.random() > 0.8 else '0',
        'RESIDIVIS_COUNTER': random.randint(0, 5),
        'NAMA_LENGKAP': name,
        'NIK': nik,
        'NAMA_ALIAS1': fake.first_name() if random.random() > 0.8 else None,
        'NAMA_ALIAS2': None,
        'NAMA_ALIAS3': None,
        'NAMA_KECIL1': fake.first_name(),
        'NAMA_KECIL2': None,
        'NAMA_KECIL3': None,
        'TANGGAL_LAHIR': dob,
        'IS_WBP_BERESIKO_TINGGI': 1 if random.random() > 0.9 else 0,
        'IS_PENGARUH_TERHADAP_MASYARAKAT': 1 if random.random() > 0.9 else 0,
        'ALAMAT': fake.address(),
        'ALAMAT_ALTERNATIF': None,
        'KODEPOS': fake.postcode(),
        'TELEPON': fake.phone_number(),
        'ALAMAT_PEKERJAAN': fake.address() if random.random() > 0.5 else None,
        'KETERANGAN_PEKERJAAN': fake.job(),
        'MINAT': fake.word(),
        'NM_AYAH': fake.name_male(),
        'TMP_TGL_AYAH': f"{fake.city()}, {fake.date()}",
        'NM_IBU': fake.name_female(),
        'TMP_TGL_IBU': f"{fake.city()}, {fake.date()}",
        'NM_SAUDARA': fake.name(),
        'ANAKKE': random.randint(1, 10),
        'JML_SAUDARA': random.randint(1, 10),
        'JML_ISTRI_SUAMI': random.randint(0, 2),
        'NM_ISTRI_SUAMI': fake.name(),
        'TMP_TGL_ISTRI_SUAMI': f"{fake.city()}, {fake.date()}",
        'JML_ANAK': random.randint(0, 5),
        'NM_ANAK': fake.name(),
        'TELEPHONE_KELUARGA': fake.phone_number(),
        'TINGGI': random.randint(150, 190),
        'BERAT': random.randint(45, 100),
        'CACAT': None,
        'CIRI': None,
        'FOTO_DEPAN': None,
        'FOTO_KANAN': None,
        'FOTO_KIRI': None,
        'FOTO_CIRI_1': None,
        'FOTO_CIRI_2': None,
        'FOTO_CIRI_3': None,
        'KONSOLIDASI': 0,
        'KONSOLIDASI_IMAGE': 0,
        'ID_KACAMATA': str(random.randint(1, 5)),
        'ID_TELINGA': str(random.randint(1, 5)),
        'ID_WARNAKULIT': str(random.randint(1, 5)),
        'ID_BENTUKRAMBUT': str(random.randint(1, 5)),
        'ID_BENTUKBIBIR': str(random.randint(1, 5)),
        'ID_LENGAN': str(random.randint(1, 5)),
        'NOMOR_INDUK_NASIONAL': generate_nik(dob, gender),
        'IS_VERIFIKASI': 1,
        'IS_DELETED': 0,
        'CREATED': fake.date_time_this_decade(),
        'CREATED_BY': 'admin',
        'UPDATED': fake.date_time_this_year(),
        'UPDATED_BY': 'admin',
        'ID_UPT': keys['ID_UPT']
    }

def ensure_table_exists(engine):
    """
    Check if the 'identitas' table exists. If not, create it.
    """
    inspector = inspect(engine)
    if not inspector.has_table("identitas"):
        print("Table 'identitas' not found. Creating it now...")
        
        try:
            with open("database/init/01-schema.sql", "r") as f:
                schema_sql = f.read()
                
            statements = schema_sql.split(';')
            with engine.begin() as conn:
                for statement in statements:
                    statement = statement.strip()
                    if statement and not statement.upper().startswith("USE") and not statement.upper().startswith("CREATE DATABASE"):
                        conn.execute(text(statement))
            print("Table 'identitas' created successfully.")
        except Exception as e:
            print(f"Failed to create table: {e}")
            _create_table_fallback(engine)
    else:
        # Check if ID_UPT column exists, if not add it
        columns = [c['name'] for c in inspector.get_columns("identitas")]
        if "ID_UPT" not in columns:
            print("Adding missing column 'ID_UPT' to 'identitas'...")
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE identitas ADD COLUMN ID_UPT VARCHAR(50)"))

def _create_table_fallback(engine):
    sql = """
    CREATE TABLE IF NOT EXISTS identitas (
        NOMOR_INDUK VARCHAR(50) NOT NULL PRIMARY KEY,
        ID_JENIS_SUKU VARCHAR(50),
        ID_JENIS_SUKU_LAIN VARCHAR(50),
        ID_JENIS_RAMBUT VARCHAR(50),
        ID_JENIS_MUKA VARCHAR(50),
        ID_JENIS_PENDIDIKAN VARCHAR(50),
        ID_JENIS_TANGAN VARCHAR(50),
        ID_JENIS_AGAMA VARCHAR(50),
        ID_JENIS_AGAMA_LAIN VARCHAR(50),
        ID_JENIS_PEKERJAAN VARCHAR(50),
        ID_JENIS_PEKERJAAN_LAIN VARCHAR(50),
        ID_USER VARCHAR(50),
        ID_BENTUK_MATA VARCHAR(50),
        ID_WARNA_MATA VARCHAR(50),
        ID_JENIS_KEAHLIAN_1 VARCHAR(50),
        ID_JENIS_KEAHLIAN_1_LAIN VARCHAR(50),
        ID_JENIS_KEAHLIAN_2 VARCHAR(50),
        ID_JENIS_KEAHLIAN_2_LAIN VARCHAR(50),
        ID_JENIS_HIDUNG VARCHAR(50),
        ID_JENIS_LEVEL_1 VARCHAR(50),
        ID_JENIS_MULUT VARCHAR(50),
        ID_JENIS_LEVEL_2 VARCHAR(50),
        ID_JENIS_WARGANEGARA VARCHAR(50),
        ID_NEGARA_ASING VARCHAR(50),
        ID_PROPINSI VARCHAR(50),
        ID_PROPINSI_LAIN VARCHAR(50),
        ID_JENIS_STATUS_PERKAWINAN VARCHAR(50),
        ID_JENIS_KELAMIN VARCHAR(50),
        ID_JENIS_KAKI VARCHAR(50),
        ID_TEMPAT_LAHIR VARCHAR(50),
        ID_TEMPAT_LAHIR_LAIN VARCHAR(50),
        ID_KOTA VARCHAR(50),
        ID_KOTA_LAIN VARCHAR(50),
        ID_TEMPAT_ASAL VARCHAR(50),
        ID_TEMPAT_ASAL_LAIN VARCHAR(50),
        RESIDIVIS VARCHAR(50),
        RESIDIVIS_COUNTER INT,
        NAMA_LENGKAP VARCHAR(255) NOT NULL,
        NIK VARCHAR(50),
        NAMA_ALIAS1 VARCHAR(255),
        NAMA_ALIAS2 VARCHAR(255),
        NAMA_ALIAS3 VARCHAR(255),
        NAMA_KECIL1 VARCHAR(255),
        NAMA_KECIL2 VARCHAR(255),
        NAMA_KECIL3 VARCHAR(255),
        TANGGAL_LAHIR DATE,
        IS_WBP_BERESIKO_TINGGI TINYINT,
        IS_PENGARUH_TERHADAP_MASYARAKAT TINYINT,
        ALAMAT TEXT,
        ALAMAT_ALTERNATIF TEXT,
        KODEPOS VARCHAR(10),
        TELEPON VARCHAR(50),
        ALAMAT_PEKERJAAN TEXT,
        KETERANGAN_PEKERJAAN TEXT,
        MINAT TEXT,
        NM_AYAH VARCHAR(255),
        TMP_TGL_AYAH VARCHAR(255),
        NM_IBU VARCHAR(255),
        TMP_TGL_IBU VARCHAR(255),
        NM_SAUDARA VARCHAR(255),
        ANAKKE INT,
        JML_SAUDARA INT,
        JML_ISTRI_SUAMI INT,
        NM_ISTRI_SUAMI VARCHAR(255),
        TMP_TGL_ISTRI_SUAMI VARCHAR(255),
        JML_ANAK INT,
        NM_ANAK VARCHAR(255),
        TELEPHONE_KELUARGA VARCHAR(50),
        TINGGI INT,
        BERAT INT,
        CACAT VARCHAR(255),
        CIRI VARCHAR(255),
        FOTO_DEPAN VARCHAR(255),
        FOTO_KANAN VARCHAR(255),
        FOTO_KIRI VARCHAR(255),
        FOTO_CIRI_1 VARCHAR(255),
        FOTO_CIRI_2 VARCHAR(255),
        FOTO_CIRI_3 VARCHAR(255),
        KONSOLIDASI INT,
        KONSOLIDASI_IMAGE INT,
        ID_KACAMATA VARCHAR(50),
        ID_TELINGA VARCHAR(50),
        ID_WARNAKULIT VARCHAR(50),
        ID_BENTUKRAMBUT VARCHAR(50),
        ID_BENTUKBIBIR VARCHAR(50),
        ID_LENGAN VARCHAR(50),
        NOMOR_INDUK_NASIONAL VARCHAR(50),
        IS_VERIFIKASI TINYINT,
        IS_DELETED TINYINT DEFAULT 0,
        CREATED DATETIME,
        CREATED_BY VARCHAR(50),
        UPDATED DATETIME,
        UPDATED_BY VARCHAR(50),
        ID_UPT VARCHAR(50)
    ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;
    """
    with engine.begin() as conn:
        conn.execute(text(sql))
    print("Table 'identitas' created via fallback method.")

def _get_current_count(engine) -> int:
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM identitas"))
            return result.scalar() or 0
    except Exception:
        return 0

def seed_command(count: int = 1000, duplicates: float = 0.0, batch_size: int = 1000):
    """
    Seed the database with synthetic data.
    """
    engine = get_engine()
    
    # Try connecting
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as e:
        print(f"Error connecting to database: {e}")
        print("Make sure Docker is running: docker-compose up -d")
        return

    # Safety Check: Prevent accidental seeding of production DB
    db_url = str(engine.url)
    is_safe = False
    safe_hosts = ["localhost", "127.0.0.1", "db", "test", "0.0.0.0"]
    
    # Extract host from URL
    # db_url format: dialect+driver://user:pass@host:port/db
    if "@" in db_url:
        host = db_url.split("@")[1].split(":")[0]
    else:
        # SQLite or similar
        host = "localhost"
        
    if host in safe_hosts:
        is_safe = True
        
    if not is_safe:
        # Check if user explicitly allowed it via config or env var?
        # Requirement: ABORT IMMEDIATELY.
        typer.secho(f"DANGER: Seeding is only allowed on local databases ({safe_hosts}).", fg=typer.colors.RED, bold=True)
        typer.secho(f"Current Host: {host}", fg=typer.colors.RED)
        typer.secho("Aborting operation to protect production data.", fg=typer.colors.RED, bold=True)
        raise typer.Abort()

    # Check and create table if needed
    ensure_table_exists(engine)

    print(f"Seeding {count} records with {duplicates*100}% duplicates...")
    
    batch = []
    recent_records = []
    MAX_RECENT = 1000
    
    # Start ID based on existing count to avoid collisions and continue sequence
    start_id = _get_current_count(engine)

    for i in track(range(count), description="Generating records..."):
        current_id = start_id + i
        
        # Decide if we generate a duplicate
        if recent_records and random.random() < duplicates:
            # Pick a record to duplicate
            original = random.choice(recent_records)
            record = original.copy()
            
            # Apply slight modifications to simulate realistic duplicates
            if random.random() > 0.5:
                 # Typo in name: swap two characters
                 name = list(record['NAMA_LENGKAP'])
                 if len(name) > 3:
                     idx = random.randint(0, len(name)-2)
                     name[idx], name[idx+1] = name[idx+1], name[idx]
                     record['NAMA_LENGKAP'] = "".join(name)
            
            if random.random() > 0.5:
                # Different address
                record['ALAMAT'] = fake.address()
                
            # Must have unique PK though, and consistent ID_UPT
            new_keys = generate_identity_keys(current_id)
            record['NOMOR_INDUK'] = new_keys['NOMOR_INDUK']
            record['ID_UPT'] = new_keys['ID_UPT']
            
        else:
            record = generate_base_record(current_id)
        
        batch.append(record)
        recent_records.append(record)
        
        if len(recent_records) > MAX_RECENT:
            recent_records.pop(0)
            
        if len(batch) >= batch_size:
            _insert_batch(engine, batch)
            batch = []
            
    if batch:
        _insert_batch(engine, batch)
        
    print("Seeding complete.")

def _insert_batch(engine, records: List[Dict]):
    if not records:
        return
        
    keys = list(records[0].keys())
    # Construct INSERT statement
    columns = ', '.join(keys)
    placeholders = ', '.join([f":{k}" for k in keys])
    sql = text(f"INSERT INTO identitas ({columns}) VALUES ({placeholders})")
    
    with engine.begin() as conn:
        conn.execute(sql, records)
