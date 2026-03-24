import logging
import traceback
import psycopg2
from db.session import get_db_connection

logger = logging.getLogger(__name__)

def init_db():
    """Initializes the database by creating all necessary tables."""
    commands = [
        # 1. Users table
        """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            user_type VARCHAR(50) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        # 2. Refresh tokens
        """
        CREATE TABLE IF NOT EXISTS refresh_tokens (
            id SERIAL PRIMARY KEY,
            token TEXT UNIQUE NOT NULL,
            user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
            expires_at TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        # 3. Event forms
        """
        CREATE TABLE IF NOT EXISTS event_forms (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            contact VARCHAR(20) NOT NULL,
            event_date DATE NOT NULL,
            event_time TIME NOT NULL,
            event_type VARCHAR(100) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        # 4. Files
        """
        CREATE TABLE IF NOT EXISTS files (
            id SERIAL PRIMARY KEY,
            filename VARCHAR(255) NOT NULL,
            file_type VARCHAR(50) NOT NULL,
            category VARCHAR(100) NOT NULL,
            file_url TEXT NOT NULL,
            uploaded_by INTEGER REFERENCES users(id),
            uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        # 5. Private files
        """
        CREATE TABLE IF NOT EXISTS private_files (
            private_files_id SERIAL PRIMARY KEY,
            uploaded_by INTEGER REFERENCES users(id),
            category VARCHAR(100) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        # 6. Private files URL
        """
        CREATE TABLE IF NOT EXISTS private_files_url (
            id SERIAL PRIMARY KEY,
            private_files_id INTEGER REFERENCES private_files(private_files_id) ON DELETE CASCADE,
            file_type VARCHAR(50) NOT NULL,
            file_url TEXT NOT NULL,
            user_selected_files TEXT,
            uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        # 7. Product frames
        """
        CREATE TABLE IF NOT EXISTS product_frames (
            id SERIAL PRIMARY KEY,
            frame_name VARCHAR(255) NOT NULL,
            phone_number VARCHAR(20) NOT NULL,
            frame_size VARCHAR(50) NOT NULL,
            user_photo_urls TEXT[],
            frame_color_urls TEXT[],
            uploaded_by INTEGER REFERENCES users(id),
            uploaded_by_type VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        # 8. Matrimony profiles
        """
        CREATE TABLE IF NOT EXISTS matrimony_profiles (
            matrimony_id VARCHAR(50) PRIMARY KEY,
            full_name VARCHAR(255) NOT NULL,
            age INTEGER,
            gender VARCHAR(10),
            date_of_birth DATE,
            email VARCHAR(255) UNIQUE NOT NULL,
            password TEXT NOT NULL,
            phone_number VARCHAR(20),
            height VARCHAR(20),
            weight VARCHAR(20),
            occupation VARCHAR(255),
            annual_income VARCHAR(50),
            education VARCHAR(255),
            mother_tongue VARCHAR(100),
            profile_created_by VARCHAR(100),
            address TEXT,
            work_type VARCHAR(100),
            company VARCHAR(255),
            work_location VARCHAR(255),
            work_country VARCHAR(100),
            mother_name VARCHAR(255),
            father_name VARCHAR(255),
            sibling_count VARCHAR(50),
            elder_brother VARCHAR(50),
            elder_sister VARCHAR(50),
            younger_sister VARCHAR(50),
            younger_brother VARCHAR(50),
            native VARCHAR(255),
            mother_occupation VARCHAR(255),
            father_occupation VARCHAR(255),
            religion VARCHAR(100),
            caste VARCHAR(100),
            sub_caste VARCHAR(100),
            nakshatra VARCHAR(100),
            rashi VARCHAR(100),
            birth_time TIME,
            birth_place VARCHAR(255),
            ascendent VARCHAR(100),
            user_type VARCHAR(50),
            preferred_age_min INTEGER,
            preferred_age_max INTEGER,
            preferred_height_min VARCHAR(20),
            preferred_height_max VARCHAR(20),
            preferred_religion VARCHAR(100),
            preferred_caste VARCHAR(100),
            preferred_sub_caste VARCHAR(100),
            preferred_nakshatra VARCHAR(100),
            preferred_rashi VARCHAR(100),
            preferred_location VARCHAR(255),
            preferred_work_status VARCHAR(100),
            photo_path TEXT,
            photos TEXT,
            horoscope_documents TEXT,
            dhosham VARCHAR(100),
            other_dhosham TEXT,
            quarter VARCHAR(50),
            marital_status VARCHAR(100),
            blood_group VARCHAR(20),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_verified BOOLEAN DEFAULT FALSE,
            is_active BOOLEAN DEFAULT TRUE,
            verification_status VARCHAR(50) DEFAULT 'pending',
            verification_comment TEXT,
            is_email_verified BOOLEAN DEFAULT FALSE,
            id SERIAL
        );
        """,
        # 9. Blocked users
        """
        CREATE TABLE IF NOT EXISTS blocked_users (
            id SERIAL PRIMARY KEY,
            blocked_matrimony_id VARCHAR(50) UNIQUE REFERENCES matrimony_profiles(matrimony_id) ON DELETE CASCADE,
            reason TEXT,
            blocked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_blocked BOOLEAN DEFAULT TRUE
        );
        """,
        # 10. Matrimony refresh tokens
        """
        CREATE TABLE IF NOT EXISTS matrimony_refresh_tokens (
            id SERIAL PRIMARY KEY,
            matrimony_id VARCHAR(50) REFERENCES matrimony_profiles(matrimony_id) ON DELETE CASCADE,
            token TEXT UNIQUE NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            is_valid BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        # 11. Matrimony ID tracker
        """
        CREATE TABLE IF NOT EXISTS matrimony_id_tracker (
            id SERIAL PRIMARY KEY,
            last_matrimony_id VARCHAR(50),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        # 12. OTP Storage
        """
        CREATE TABLE IF NOT EXISTS otp_storage (
            id SERIAL PRIMARY KEY,
            mobile_number VARCHAR(20) NOT NULL,
            full_name VARCHAR(255),
            otp VARCHAR(6) NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        # 13. Deleted profiles
        """
        CREATE TABLE IF NOT EXISTS deleted_profiles (
            LIKE matrimony_profiles INCLUDING ALL
        );
        """,
        # 14. Deactivation reports
        """
        CREATE TABLE IF NOT EXISTS deactivation_reports (
            id SERIAL PRIMARY KEY,
            matrimony_id VARCHAR(50) REFERENCES matrimony_profiles(matrimony_id),
            reason TEXT,
            reported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        # 15. User wallets
        """
        CREATE TABLE IF NOT EXISTS user_wallets (
            id SERIAL PRIMARY KEY,
            matrimony_id VARCHAR(50) UNIQUE REFERENCES matrimony_profiles(matrimony_id),
            balance INTEGER DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        # 16. Spend actions
        """
        CREATE TABLE IF NOT EXISTS spend_actions (
            id SERIAL PRIMARY KEY,
            matrimony_id VARCHAR(50) REFERENCES matrimony_profiles(matrimony_id),
            profile_matrimony_id VARCHAR(50) REFERENCES matrimony_profiles(matrimony_id),
            points INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        # 17. Favorite profiles
        """
        CREATE TABLE IF NOT EXISTS favorite_profiles (
            id SERIAL PRIMARY KEY,
            matrimony_id VARCHAR(50) REFERENCES matrimony_profiles(matrimony_id),
            favorite_matrimony_id VARCHAR(50) REFERENCES matrimony_profiles(matrimony_id),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(matrimony_id, favorite_matrimony_id)
        );
        """,
        # 18. Chat messages
        """
        CREATE TABLE IF NOT EXISTS chat_messages (
            id SERIAL PRIMARY KEY,
            sender_id VARCHAR(50),
            receiver_id VARCHAR(50),
            message TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        # 19. Reported profiles
        """
        CREATE TABLE IF NOT EXISTS reported_profiles (
            id SERIAL PRIMARY KEY,
            reporter_matrimony_id VARCHAR(50) REFERENCES matrimony_profiles(matrimony_id),
            reported_matrimony_id VARCHAR(50) REFERENCES matrimony_profiles(matrimony_id),
            reason TEXT,
            reported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        # 20. Contact us
        """
        CREATE TABLE IF NOT EXISTS contact_us (
            id SERIAL PRIMARY KEY,
            full_name VARCHAR(255) NOT NULL,
            email VARCHAR(255) NOT NULL,
            message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        # 21. Viewed profiles
        """
        CREATE TABLE IF NOT EXISTS viewed_profiles (
            id SERIAL PRIMARY KEY,
            viewer_id VARCHAR(50) REFERENCES matrimony_profiles(matrimony_id),
            viewed_id VARCHAR(50) REFERENCES matrimony_profiles(matrimony_id),
            viewed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(viewer_id, viewed_id)
        );
        """
    ]

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        for command in commands:
            cur.execute(command)
        
        # --- Column Migration Logic ---
        # Ensure new columns are added to existing tables
        migrations = [
            ("matrimony_profiles", [
                ("verification_status", "VARCHAR(50) DEFAULT 'pending'"),
                ("verification_comment", "TEXT"),
                ("is_email_verified", "BOOLEAN DEFAULT FALSE"),
                ("id", "SERIAL")
            ]),
            ("deleted_profiles", [
                ("verification_status", "VARCHAR(50) DEFAULT 'pending'"),
                ("verification_comment", "TEXT"),
                ("is_email_verified", "BOOLEAN DEFAULT FALSE"),
                ("id", "SERIAL")
            ]),
            ("matrimony_refresh_tokens", [
                ("is_valid", "BOOLEAN DEFAULT TRUE")
            ]),
            ("spend_actions", [
                ("matrimony_id", "VARCHAR(50) REFERENCES matrimony_profiles(matrimony_id)")
            ])
        ]
        
        for table, cols in migrations:
            # Check if table exists
            cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)", (table,))
            if cur.fetchone()[0]:
                for col_name, col_def in cols:
                    try:
                        cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col_name} {col_def}")
                        logger.info(f"Ensured column {col_name} exists in {table}")
                    except Exception as e:
                        logger.warning(f"Could not add column {col_name} to {table}: {e}")

        try:
            cur.execute("""
                ALTER TABLE event_forms 
                ALTER COLUMN event_time TYPE VARCHAR(50)
                USING event_time::VARCHAR;
            """)
            logger.info("event_time column updated to VARCHAR(50)")
        except Exception as e:
            conn.rollback()
            logger.info(f"event_time column already VARCHAR or skipped: {e}")

        conn.commit()
        print("[✓] Database initialized and migrated successfully.")
        logger.info("Database initialized and migrated successfully.")
    except Exception as e:
        if 'conn' in locals() and conn:
            conn.rollback()
        logger.error(f"Database initialization failed: {str(e)}")
        logger.error(traceback.format_exc())
        raise e
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()
