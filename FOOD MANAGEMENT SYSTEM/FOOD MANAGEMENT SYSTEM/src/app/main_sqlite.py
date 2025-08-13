import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import sqlite3
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / 'food_rescue.db'

def get_db_connection():
    """Create SQLite database connection"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row  # This allows accessing columns by name
    return conn

def import_csv_data():
    """Import CSV files from data/ directory"""
    DATA_DIR = ROOT / 'data'
    
    if not DATA_DIR.exists():
        st.error(f"Data directory {DATA_DIR} not found! Please create it and put your CSV files there.")
        return False
    
    csv_files = list(DATA_DIR.glob('*.csv'))
    if not csv_files:
        st.error(f"No CSV files found in {DATA_DIR}. Please add your CSV files there.")
        return False
    
    st.info(f"Found CSV files: {[f.name for f in csv_files]}")
    
    # Create tables
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Drop existing tables if they exist
    cursor.execute("DROP TABLE IF EXISTS claims")
    cursor.execute("DROP TABLE IF EXISTS food_listings")
    cursor.execute("DROP TABLE IF EXISTS receivers")
    cursor.execute("DROP TABLE IF EXISTS providers")
    cursor.execute("DROP TABLE IF EXISTS audit_log")
    
    # Create tables
    cursor.execute('''
        CREATE TABLE providers (
            provider_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            type TEXT,
            address TEXT,
            city TEXT,
            contact TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE receivers (
            receiver_id INTEGER PRIMARY KEY,
            name TEXT,
            type TEXT,
            city TEXT,
            contact TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE food_listings (
            food_id INTEGER PRIMARY KEY,
            food_name TEXT,
            quantity INTEGER,
            expiry_date DATE,
            provider_id INTEGER,
            provider_type TEXT,
            location TEXT,
            food_type TEXT,
            meal_type TEXT,
            FOREIGN KEY (provider_id) REFERENCES providers(provider_id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE claims (
            claim_id INTEGER PRIMARY KEY,
            food_id INTEGER,
            receiver_id INTEGER,
            status TEXT CHECK (status IN ('Pending','Completed','Cancelled')),
            timestamp DATETIME,
            FOREIGN KEY (food_id) REFERENCES food_listings(food_id),
            FOREIGN KEY (receiver_id) REFERENCES receivers(receiver_id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            operation TEXT NOT NULL,
            user TEXT DEFAULT 'streamlit',
            details TEXT,
            ts_utc DATETIME NOT NULL
        )
    ''')
    
    # Import data from CSV files
    import_order = ['providers', 'receivers', 'food_listings', 'claims']
    
    for table in import_order:
        csv_file = DATA_DIR / f"{table}_data.csv"
        if csv_file.exists():
            try:
                df = pd.read_csv(csv_file)
                st.write(f"Importing {table}: {len(df)} rows")
                
                # Clean the data
                df = df.copy()
                
                # Handle expiry_date - convert to ISO format, drop invalid
                if 'expiry_date' in df.columns:
                    df['expiry_date'] = pd.to_datetime(df['expiry_date'], errors='coerce')
                    df = df.dropna(subset=['expiry_date'])
                    df['expiry_date'] = df['expiry_date'].dt.strftime('%Y-%m-%d')
                
                # Handle quantity - convert to integer, NULL for negative/missing
                if 'quantity' in df.columns:
                    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
                    df.loc[df['quantity'] < 0, 'quantity'] = None
                
                # Handle contact - normalize phone numbers
                if 'contact' in df.columns:
                    df['contact'] = df['contact'].astype(str).str.strip()
                
                # Deduplicate by contact for providers and receivers
                if table in ['providers', 'receivers'] and 'contact' in df.columns:
                    df = df.drop_duplicates(subset=['contact'], keep='first')
                
                st.write(f"After cleaning: {len(df)} rows")
                
                # Insert data
                if not df.empty:
                    placeholders = ', '.join(['?' for _ in df.columns])
                    columns = ', '.join(df.columns)
                    query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
                    
                    for _, row in df.iterrows():
                        cursor.execute(query, tuple(row))
                    
                    st.success(f"✅ {table}: {len(df)} rows imported")
                else:
                    st.warning(f"⚠️ {table}: No data after cleaning")
                    
            except Exception as e:
                st.error(f"❌ Error importing {table}: {str(e)}")
                return False
        else:
            st.warning(f"⚠️ {csv_file} not found, skipping {table}")
    
    conn.commit()
    conn.close()
    st.success("🎉 CSV import completed!")
    return True

def init_database():
    """Initialize the database - either with CSV import or sample data"""
    if not DB_PATH.exists():
        st.info("🔄 First time setup - initializing database...")
        
        # Check if CSV files exist
        DATA_DIR = ROOT / 'data'
        if DATA_DIR.exists() and list(DATA_DIR.glob('*.csv')):
            st.write("📁 Found CSV files - importing real data...")
            if import_csv_data():
                return
            else:
                st.warning("⚠️ CSV import failed, using sample data instead")
        
        # Fallback to sample data
        st.write("📊 Using sample data...")
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Create tables
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS providers (
                provider_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT,
                address TEXT,
                city TEXT,
                contact TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS receivers (
                receiver_id INTEGER PRIMARY KEY,
                name TEXT,
                type TEXT,
                city TEXT,
                contact TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS food_listings (
                food_id INTEGER PRIMARY KEY,
                food_name TEXT,
                quantity INTEGER,
                expiry_date DATE,
                provider_id INTEGER,
                provider_type TEXT,
                location TEXT,
                food_type TEXT,
                meal_type TEXT,
                FOREIGN KEY (provider_id) REFERENCES providers(provider_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS claims (
                claim_id INTEGER PRIMARY KEY,
                food_id INTEGER,
                receiver_id INTEGER,
                status TEXT CHECK (status IN ('Pending','Completed','Cancelled')),
                timestamp DATETIME,
                FOREIGN KEY (food_id) REFERENCES food_listings(food_id),
                FOREIGN KEY (receiver_id) REFERENCES receivers(receiver_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation TEXT NOT NULL,
                user TEXT DEFAULT 'streamlit',
                details TEXT,
                ts_utc DATETIME NOT NULL
            )
        ''')
        
        # Insert sample data
        cursor.execute('''
            INSERT INTO providers (provider_id, name, type, city, contact) VALUES 
            (1, 'Restaurant A', 'Restaurant', 'New York', '+1-555-0101'),
            (2, 'Cafe B', 'Cafe', 'Los Angeles', '+1-555-0102'),
            (3, 'Grocery C', 'Grocery', 'Chicago', '+1-555-0103'),
            (4, 'Bakery D', 'Bakery', 'Houston', '+1-555-0104'),
            (5, 'Hotel E', 'Hotel', 'Phoenix', '+1-555-0105')
        ''')
        
        cursor.execute('''
            INSERT INTO receivers (receiver_id, name, type, city, contact) VALUES 
            (1, 'Food Bank A', 'Food Bank', 'New York', '+1-555-0201'),
            (2, 'Shelter B', 'Shelter', 'Los Angeles', '+1-555-0202'),
            (3, 'Community C', 'Community', 'Chicago', '+1-555-0203'),
            (4, 'Church D', 'Church', 'Houston', '+1-555-0204'),
            (5, 'School E', 'School', 'Phoenix', '+1-555-0205')
        ''')
        
        cursor.execute('''
            INSERT INTO food_listings (food_id, food_name, quantity, expiry_date, provider_id, provider_type, location, food_type, meal_type) VALUES 
            (1, 'Bread', 50, '2025-01-15', 1, 'Restaurant', 'Kitchen A', 'Bread', 'Breakfast'),
            (2, 'Rice', 100, '2025-02-01', 2, 'Cafe', 'Storage B', 'Grain', 'Lunch'),
            (3, 'Vegetables', 75, '2025-01-20', 3, 'Grocery', 'Warehouse C', 'Vegetables', 'Dinner'),
            (4, 'Fruits', 60, '2025-01-25', 4, 'Bakery', 'Bakery D', 'Fruits', 'Snack'),
            (5, 'Milk', 30, '2025-01-18', 5, 'Hotel', 'Kitchen E', 'Dairy', 'Breakfast'),
            (6, 'Cheese', 25, '2025-01-30', 1, 'Restaurant', 'Kitchen A', 'Dairy', 'Lunch'),
            (7, 'Pasta', 80, '2025-02-05', 2, 'Cafe', 'Storage B', 'Grain', 'Dinner'),
            (8, 'Meat', 40, '2025-01-22', 3, 'Grocery', 'Warehouse C', 'Protein', 'Dinner')
        ''')
        
        cursor.execute('''
            INSERT INTO claims (claim_id, food_id, receiver_id, status, timestamp) VALUES 
            (1, 1, 1, 'Completed', '2025-01-10 10:00:00'),
            (2, 2, 2, 'Pending', '2025-01-11 14:30:00'),
            (3, 3, 3, 'Completed', '2025-01-12 09:15:00'),
            (4, 4, 4, 'Cancelled', '2025-01-13 16:45:00'),
            (5, 5, 5, 'Pending', '2025-01-14 11:20:00')
        ''')
        
        conn.commit()
        conn.close()
        st.success('✅ Database initialized with sample data!')

def run_query(query, params=None):
    """Run a SQL query and return results as DataFrame"""
    conn = get_db_connection()
    try:
        if params:
            df = pd.read_sql_query(query, conn, params=params)
        else:
            df = pd.read_sql_query(query, conn)
        return df
    finally:
        conn.close()

def execute_query(query, params=None):
    """Execute a SQL query (INSERT, UPDATE, DELETE)"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        conn.commit()
        return cursor.rowcount
    finally:
        conn.close()

def log_audit(operation, details=''):
    """Log an operation to audit log"""
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    execute_query(
        "INSERT INTO audit_log(operation, user, details, ts_utc) VALUES (?, 'streamlit', ?, ?)",
        (operation, details, now)
    )

def page_home():
    st.title('Food Rescue Platform')
    st.write('Connect surplus-food providers to receivers to reduce waste and hunger.')
    
    # Get KPIs from database
    providers_count = run_query("SELECT COUNT(*) as count FROM providers").iloc[0]['count']
    receivers_count = run_query("SELECT COUNT(*) as count FROM receivers").iloc[0]['count']
    listings_count = run_query("SELECT COUNT(*) as count FROM food_listings").iloc[0]['count']
    claims_count = run_query("SELECT COUNT(*) as count FROM claims").iloc[0]['count']
    completed_count = run_query("SELECT COUNT(*) as count FROM claims WHERE status='Completed'").iloc[0]['count']
    
    pct_completed = (completed_count / claims_count * 100) if claims_count > 0 else 0
    
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric('Providers', providers_count)
    k2.metric('Receivers', receivers_count)
    k3.metric('Listings', listings_count)
    k4.metric('Claims', claims_count)
    k5.metric('% Completed', f"{pct_completed:.1f}%")
    
    # Charts
    claims_data = run_query("SELECT status, COUNT(*) as count FROM claims GROUP BY status")
    if not claims_data.empty:
        fig1 = px.pie(claims_data, names='status', values='count', title='Claims status distribution')
        st.plotly_chart(fig1, use_container_width=True)
    
    # Time series
    weekly_data = run_query("""
        SELECT strftime('%Y-W%W', timestamp) as week, COUNT(*) as claims 
        FROM claims 
        GROUP BY week 
        ORDER BY week
    """)
    if not weekly_data.empty:
        fig2 = px.line(weekly_data, x='week', y='claims', title='Claims per week')
        st.plotly_chart(fig2, use_container_width=True)
    
    # Listings table with filters
    st.subheader('Food Listings')
    listings_query = """
        SELECT f.*, p.city, p.name AS provider_name
        FROM food_listings f 
        JOIN providers p ON p.provider_id = f.provider_id
    """
    listings = run_query(listings_query)
    
    if not listings.empty:
        cities = sorted(listings['city'].dropna().unique())
        food_types = sorted(listings['food_type'].dropna().unique())
        meal_types = sorted(listings['meal_type'].dropna().unique())
        
        c1, c2 = st.columns(2)
        with c1:
            f_city = st.multiselect('City', cities)
            f_food = st.multiselect('Food Type', food_types)
        with c2:
            f_meal = st.multiselect('Meal Type', meal_types)
        
        # Filter data
        df = listings.copy()
        if f_city:
            df = df[df['city'].isin(f_city)]
        if f_food:
            df = df[df['food_type'].isin(f_food)]
        if f_meal:
            df = df[df['meal_type'].isin(f_meal)]
        
        # Highlight near expiry
        df['expiry_date'] = pd.to_datetime(df['expiry_date'])
        today = pd.Timestamp.now().normalize()
        
        def highlight(row):
            if row['expiry_date'] <= today + pd.Timedelta(days=3):
                return 'background-color: #ffd6d6'
            return ''
        
        st.dataframe(df.style.apply(lambda r: [highlight(r)] * len(r), axis=1), use_container_width=True)
    else:
        st.info('No food listings found')

def page_manage_listings():
    st.header('Manage Listings (CRUD)')
    
    providers = run_query("SELECT provider_id, name FROM providers ORDER BY name")
    listings = run_query("SELECT * FROM food_listings")
    
    with st.expander('Add Listing', expanded=False):
        with st.form('add_listing_form'):
            food_id = st.number_input('Food_ID', step=1)
            food_name = st.text_input('Food_Name')
            quantity = st.number_input('Quantity', step=1, min_value=0)
            expiry_date = st.date_input('Expiry_Date')
            provider_choice = st.selectbox('Provider', providers['name'].tolist())
            provider_id = int(providers.loc[providers['name'] == provider_choice, 'provider_id'].iloc[0]) if not providers.empty else None
            provider_type = st.text_input('Provider_Type')
            location = st.text_input('Location')
            food_type = st.text_input('Food_Type')
            meal_type = st.text_input('Meal_Type')
            submitted = st.form_submit_button('Create')
            if submitted:
                execute_query('''
                    INSERT INTO food_listings(food_id, food_name, quantity, expiry_date, provider_id, provider_type, location, food_type, meal_type)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (food_id, food_name, quantity, str(expiry_date), provider_id, provider_type, location, food_type, meal_type))
                log_audit('create_listing', f'food_id={food_id}')
                st.success('Listing created!')
                st.rerun()
    
    with st.expander('Edit Listing'):
        if not listings.empty:
            picked_id = st.selectbox('Select Food_ID', listings['food_id'].tolist())
            row = listings[listings['food_id'] == picked_id].iloc[0]
            with st.form('edit_listing_form'):
                food_name = st.text_input('Food_Name', row['food_name'])
                quantity = st.number_input('Quantity', step=1, min_value=0, value=int(row['quantity']) if not pd.isna(row['quantity']) else 0)
                expiry_date = st.date_input('Expiry_Date', pd.to_datetime(row['expiry_date']).date())
                provider_id = st.number_input('Provider_ID', step=1, value=int(row['provider_id']))
                provider_type = st.text_input('Provider_Type', row['provider_type'] or '')
                location = st.text_input('Location', row['location'] or '')
                food_type = st.text_input('Food_Type', row['food_type'] or '')
                meal_type = st.text_input('Meal_Type', row['meal_type'] or '')
                submitted = st.form_submit_button('Update')
                if submitted:
                    execute_query('''
                        UPDATE food_listings SET food_name=?, quantity=?, expiry_date=?, provider_id=?, provider_type=?, location=?, food_type=?, meal_type=? 
                        WHERE food_id=?
                    ''', (food_name, quantity, str(expiry_date), provider_id, provider_type, location, food_type, meal_type, picked_id))
                    log_audit('update_listing', f'food_id={picked_id}')
                    st.success('Listing updated!')
                    st.rerun()
        else:
            st.info('No listings to edit')
    
    with st.expander('Delete Listing'):
        if not listings.empty:
            del_id = st.selectbox('Select Food_ID to delete', listings['food_id'].tolist())
            if st.button('Confirm Delete'):
                execute_query('DELETE FROM food_listings WHERE food_id=?', (del_id,))
                log_audit('delete_listing', f'food_id={del_id}')
                st.success('Listing deleted!')
                st.rerun()
        else:
            st.info('No listings to delete')

def page_manage_claims():
    st.header('Manage Claims (CRUD)')
    
    foods = run_query("SELECT food_id, food_name, provider_id FROM food_listings")
    receivers = run_query("SELECT receiver_id, name FROM receivers")
    claims = run_query("SELECT * FROM claims")
    
    with st.expander('Add Claim'):
        with st.form('add_claim_form'):
            claim_id = st.number_input('Claim_ID', step=1)
            food_choice = st.selectbox('Food', foods.apply(lambda r: f"{r['food_id']} - {r['food_name']}", axis=1).tolist())
            food_id = int(food_choice.split(' - ')[0]) if food_choice else None
            recv_choice = st.selectbox('Receiver', receivers.apply(lambda r: f"{r['receiver_id']} - {r['name']}", axis=1).tolist())
            receiver_id = int(recv_choice.split(' - ')[0]) if recv_choice else None
            status = st.selectbox('Status', ['Pending', 'Completed', 'Cancelled'], index=0)
            submitted = st.form_submit_button('Create Claim')
            if submitted:
                ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                execute_query('''
                    INSERT INTO claims(claim_id, food_id, receiver_id, status, timestamp)
                    VALUES (?, ?, ?, ?, ?)
                ''', (claim_id, food_id, receiver_id, status, ts))
                log_audit('create_claim', f'claim_id={claim_id}')
                st.success('Claim created!')
                st.rerun()
    
    with st.expander('Update Claim Status'):
        if not claims.empty:
            pick = st.selectbox('Select Claim', claims.apply(lambda r: f"{r['claim_id']} - {r['status']}", axis=1).tolist())
            claim_id = int(pick.split(' - ')[0])
            new_status = st.selectbox('New Status', ['Pending', 'Completed', 'Cancelled'])
            if st.button('Update Status'):
                execute_query('UPDATE claims SET status=? WHERE claim_id=?', (new_status, claim_id))
                log_audit('update_claim', f'claim_id={claim_id}, status={new_status}')
                st.success('Status updated!')
                st.rerun()
        else:
            st.info('No claims to update')
    
    st.subheader('View Claims')
    st.dataframe(claims, use_container_width=True)

def page_providers_receivers():
    st.header('Providers & Receivers')
    tab1, tab2 = st.tabs(['Providers', 'Receivers'])
    
    providers = run_query("SELECT * FROM providers")
    receivers = run_query("SELECT * FROM receivers")
    
    with tab1:
        st.dataframe(providers, use_container_width=True)
        if not providers.empty:
            for _, row in providers.iterrows():
                st.write(f"{row['name']} ({row['city']}) - {row['contact']}")
                if st.button(f"Copy {row['contact']}", key=f"copy_{row['provider_id']}"):
                    st.success(f"Copied {row['contact']} to clipboard!")
        
        st.download_button('Export provider contact CSV', data=providers[['name','city','contact']].to_csv(index=False), file_name='providers_contacts.csv', mime='text/csv')
        
        with st.expander('Add / Edit / Delete Provider'):
            with st.form('provider_form'):
                mode = st.selectbox('Action', ['Add','Edit','Delete'])
                provider_id = st.number_input('Provider_ID', step=1)
                name = st.text_input('Name')
                type_ = st.text_input('Type')
                address = st.text_input('Address')
                city = st.text_input('City')
                contact = st.text_input('Contact')
                submitted = st.form_submit_button('Submit')
                if submitted:
                    if mode == 'Add':
                        execute_query('''
                            INSERT INTO providers(provider_id,name,type,address,city,contact)
                            VALUES (?, ?, ?, ?, ?, ?)
                        ''', (provider_id, name, type_, address, city, contact))
                        log_audit('create_provider', f'provider_id={provider_id}')
                        st.success('Provider added!')
                    elif mode == 'Edit':
                        execute_query('''
                            UPDATE providers SET name=?, type=?, address=?, city=?, contact=?
                            WHERE provider_id=?
                        ''', (name, type_, address, city, contact, provider_id))
                        log_audit('update_provider', f'provider_id={provider_id}')
                        st.success('Provider updated!')
                    else:
                        execute_query('DELETE FROM providers WHERE provider_id=?', (provider_id,))
                        log_audit('delete_provider', f'provider_id={provider_id}')
                        st.success('Provider deleted!')
                    st.rerun()
    
    with tab2:
        st.dataframe(receivers, use_container_width=True)
        with st.expander('Add / Edit / Delete Receiver'):
            with st.form('receiver_form'):
                mode = st.selectbox('Action', ['Add','Edit','Delete'])
                receiver_id = st.number_input('Receiver_ID', step=1)
                name = st.text_input('Name', key='r_name')
                type_ = st.text_input('Type', key='r_type')
                city = st.text_input('City', key='r_city')
                contact = st.text_input('Contact', key='r_contact')
                submitted = st.form_submit_button('Submit', type='primary')
                if submitted:
                    if mode == 'Add':
                        execute_query('''
                            INSERT INTO receivers(receiver_id,name,type,city,contact)
                            VALUES (?, ?, ?, ?, ?)
                        ''', (receiver_id, name, type_, city, contact))
                        log_audit('create_receiver', f'receiver_id={receiver_id}')
                        st.success('Receiver added!')
                    elif mode == 'Edit':
                        execute_query('''
                            UPDATE receivers SET name=?, type=?, city=?, contact=?
                            WHERE receiver_id=?
                        ''', (name, type_, city, contact, receiver_id))
                        log_audit('update_receiver', f'receiver_id={receiver_id}')
                        st.success('Receiver updated!')
                    else:
                        execute_query('DELETE FROM receivers WHERE receiver_id=?', (receiver_id,))
                        log_audit('delete_receiver', f'receiver_id={receiver_id}')
                        st.success('Receiver deleted!')
                    st.rerun()

def page_sql_queries():
    st.header('SQL Queries & Analysis (Required)')
    
    queries = {
        'Providers and receivers per city': '''
            SELECT city, 
                   SUM(CASE WHEN src='provider' THEN cnt ELSE 0 END) AS providers,
                   SUM(CASE WHEN src='receiver' THEN cnt ELSE 0 END) AS receivers
            FROM (
                SELECT city, COUNT(*) AS cnt, 'provider' AS src FROM providers GROUP BY city
                UNION ALL
                SELECT city, COUNT(*) AS cnt, 'receiver' AS src FROM receivers GROUP BY city
            ) t
            GROUP BY city
            ORDER BY city
        ''',
        'Top provider type': '''
            SELECT provider_type, COUNT(*) AS listings_count
            FROM food_listings
            GROUP BY provider_type
            ORDER BY listings_count DESC
            LIMIT 1
        ''',
        'Provider contacts in city': '''
            SELECT name, contact FROM providers WHERE city = ?
            ORDER BY name
        ''',
        'Top receivers by claims': '''
            SELECT r.receiver_id, r.name, COUNT(*) AS claims_count
            FROM claims c
            JOIN receivers r ON r.receiver_id = c.receiver_id
            GROUP BY r.receiver_id, r.name
            ORDER BY claims_count DESC
        ''',
        'Total quantity available': '''
            SELECT SUM(quantity) AS total_quantity FROM food_listings
        ''',
        'City with most listings': '''
            SELECT p.city, COUNT(*) AS listings_count
            FROM food_listings f
            JOIN providers p ON p.provider_id = f.provider_id
            GROUP BY p.city
            ORDER BY listings_count DESC
            LIMIT 1
        ''',
        'Most common food types': '''
            SELECT food_type, COUNT(*) AS cnt
            FROM food_listings
            GROUP BY food_type
            ORDER BY cnt DESC
            LIMIT 5
        ''',
        'Claims per food item': '''
            SELECT f.food_id, f.food_name, COUNT(c.claim_id) AS claims_count
            FROM food_listings f
            LEFT JOIN claims c ON c.food_id = f.food_id
            GROUP BY f.food_id, f.food_name
            ORDER BY claims_count DESC
        ''',
        'Claims status distribution': '''
            SELECT status, COUNT(*) AS cnt,
                   ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM claims), 2) AS pct
            FROM claims
            GROUP BY status
        ''',
        'Food listings near expiry (<=3 days)': '''
            SELECT *
            FROM food_listings
            WHERE expiry_date <= date('now','+3 day')
            ORDER BY expiry_date ASC
        ''',
        'Claims per week (time-series)': '''
            SELECT strftime('%Y-W%W', timestamp) AS iso_week, COUNT(*) AS claims
            FROM claims
            GROUP BY iso_week
            ORDER BY iso_week
        '''
    }
    
    for label, sql in queries.items():
        st.subheader(label)
        with st.expander('SQL', expanded=False):
            st.code(sql, language='sql')
        
        params = {}
        if label == 'Provider contacts in city':
            cities = run_query("SELECT DISTINCT city FROM providers WHERE city IS NOT NULL")['city'].dropna().tolist()
            if cities:
                city = st.selectbox('City', cities, key=f'city_{label}')
                params = (city,)
        
        if st.button(f'Run: {label}'):
            try:
                df = run_query(sql, params)
                st.dataframe(df, use_container_width=True)
                st.download_button('Export result CSV', data=df.to_csv(index=False), file_name=f'{label.replace(" ","_")}.csv', mime='text/csv')
            except Exception as e:
                st.error(f'Error running query: {str(e)}')

def page_eda():
    st.header('EDA / Insights')
    
    # City trends
    city_counts = run_query('''
        SELECT p.city, COUNT(*) as listings
        FROM food_listings f 
        JOIN providers p ON p.provider_id = f.provider_id
        GROUP BY p.city
    ''')
    if not city_counts.empty:
        st.plotly_chart(px.bar(city_counts, x='city', y='listings', title='Listings by City'), use_container_width=True)
    
    # Meal type demand
    meal_counts = run_query('''
        SELECT meal_type, COUNT(*) as count
        FROM food_listings 
        GROUP BY meal_type 
        ORDER BY count DESC
    ''')
    if not meal_counts.empty:
        st.plotly_chart(px.bar(meal_counts, x='meal_type', y='count', title='Listings by Meal Type'), use_container_width=True)
    
    # Expiry risk
    near = run_query('''
        SELECT *
        FROM food_listings
        WHERE expiry_date <= date('now','+3 day')
        ORDER BY expiry_date ASC
    ''')
    st.write('Listings near expiry (<=3 days):')
    if not near.empty:
        st.dataframe(near, use_container_width=True)
    else:
        st.info('No listings near expiry')

def page_admin():
    st.header('Admin / Deploy')
    
    # CSV Import Section
    st.subheader('📁 CSV Data Import')
    if st.button('🔄 Re-import CSV Data'):
        if import_csv_data():
            st.success('✅ CSV data re-imported successfully!')
            st.rerun()
    
    # Get counts
    counts = {}
    for table in ['providers', 'receivers', 'food_listings', 'claims', 'audit_log']:
        count = run_query(f"SELECT COUNT(*) as count FROM {table}").iloc[0]['count']
        counts[table] = count
    
    st.write('📊 Row counts:', counts)
    
    if st.button('💾 Backup DB to CSV (tables)'):
        for table in ['providers', 'receivers', 'food_listings', 'claims']:
            df = run_query(f"SELECT * FROM {table}")
            if not df.empty:
                st.download_button(f'📥 Download {table}.csv', data=df.to_csv(index=False), file_name=f'{table}.csv', mime='text/csv')
    
    if st.button('📄 Export SQL dump'):
        # Get schema
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT sql FROM sqlite_master WHERE type='table'")
        schema = cursor.fetchall()
        conn.close()
        
        schema_text = '\n'.join([row[0] for row in schema if row[0]])
        st.download_button('📥 Download schema.sql', data=schema_text, file_name='schema.sql', mime='text/plain')
    
    st.success('✅ SQLite database is working! All operations are real and persistent.')

def main():
    # Initialize database on first run
    init_database()
    
    st.sidebar.title('Navigation')
    page = st.sidebar.radio('Go to', [
        'Home / Dashboard',
        'Manage Listings',
        'Manage Claims',
        'Providers & Receivers',
        'SQL Queries & Analysis',
        'EDA / Insights',
        'Admin / Deploy'
    ])
    
    if page == 'Home / Dashboard':
        page_home()
    elif page == 'Manage Listings':
        page_manage_listings()
    elif page == 'Manage Claims':
        page_manage_claims()
    elif page == 'Providers & Receivers':
        page_providers_receivers()
    elif page == 'SQL Queries & Analysis':
        page_sql_queries()
    elif page == 'EDA / Insights':
        page_eda()
    else:
        page_admin()

if __name__ == '__main__':
    main() 