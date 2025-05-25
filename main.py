import requests
import os
import logging
import asyncio
import sys

# Set up environment-based logging
MODE = os.getenv('MODE', 'dev')

if MODE == 'prod':
    # Production: Use Cloud Logging + structured stdout
    try:
        from google.cloud import logging as cloud_logging
        import base64
        import json
        from google.oauth2 import service_account
        
        # Load GCP credentials
        credentials_b64 = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if credentials_b64:
            credentials_bytes = base64.b64decode(credentials_b64)
            credentials_dict = json.loads(credentials_bytes)
            credentials = service_account.Credentials.from_service_account_info(credentials_dict)
            
            cloud_client = cloud_logging.Client(credentials=credentials)
            cloud_client.setup_logging()
            print("✅ Google Cloud Logging integration enabled for main.py")
        else:
            print("⚠️ GOOGLE_APPLICATION_CREDENTIALS not found, using stdout only")
    except Exception as e:
        print(f"⚠️ Cloud Logging setup failed, using stdout only: {e}")
    
    # Use structured format for production
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
else:
    # Development: Use basic console logging
    logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)

def scrape_aucb(request):
    """
    Cloud Function entry point for AUCB cricket data scraping.
    Runs daily at 6 AM via Cloud Scheduler.
    On success, triggers the AUCB transformer function.
    """
    try:
        logger.info("🏏 Starting AUCB cricket data scraping...")
        
        # Import and run AUCB scraper
        from scrape.aucb_bbb_scrape import main as aucb_scrape_main
        
        logger.info("📡 Running AUCB scraper...")
        scraping_success = asyncio.run(aucb_scrape_main())
        
        if scraping_success:
            logger.info("✅ AUCB scraping completed successfully!")
            
            # Trigger AUCB transformer
            transform_url = os.environ.get('TRANSFORM_AUCB_URL')
            if transform_url:
                logger.info("🔄 Triggering AUCB transformer...")
                response = requests.post(transform_url, json={}, timeout=600)
                if response.status_code == 200:
                    logger.info("✅ AUCB transformer triggered successfully!")
                    return {"status": "success", "message": "AUCB scraping and transformation completed"}
                else:
                    logger.error(f"❌ Failed to trigger AUCB transformer: {response.status_code}")
                    return {"status": "partial_success", "message": "AUCB scraping completed but transformer failed"}
            else:
                logger.warning("⚠️ TRANSFORM_AUCB_URL not set, skipping transformer trigger")
                return {"status": "success", "message": "AUCB scraping completed (no transformer trigger)"}
        else:
            logger.error("❌ AUCB scraping failed!")
            return {"status": "error", "message": "AUCB scraping failed"}
            
    except Exception as e:
        logger.error(f"❌ AUCB scraping pipeline error: {e}")
        return {"status": "error", "message": f"AUCB scraping pipeline error: {str(e)}"}

def transform_aucb(request):
    """
    Cloud Function entry point for AUCB data transformation.
    Triggered by scrape_aucb function or can be run independently.
    """
    try:
        logger.info("🔄 Starting AUCB data transformation...")
        
        # Import and run AUCB transformer
        from transform.aucb import main as aucb_transform_main
        
        logger.info("⚙️ Running AUCB transformer...")
        transform_success = aucb_transform_main()
        
        if transform_success:
            logger.info("✅ AUCB transformation completed successfully!")
            return {"status": "success", "message": "AUCB transformation completed"}
        else:
            logger.error("❌ AUCB transformation failed!")
            return {"status": "error", "message": "AUCB transformation failed"}
            
    except Exception as e:
        logger.error(f"❌ AUCB transformation error: {e}")
        return {"status": "error", "message": f"AUCB transformation error: {str(e)}"}

def scrape_cricinfo(request):
    """
    Cloud Function entry point for Cricinfo cricket data scraping.
    Runs daily at 6 AM via Cloud Scheduler.
    On success, triggers the Cricinfo transformer function.
    """
    try:
        logger.info("🏏 Starting Cricinfo cricket data scraping...")
        
        # Import and run Cricinfo scraper
        from scrape.cricinfo_bbb_scrape import main as cricinfo_scrape_main
        
        logger.info("📡 Running Cricinfo scraper...")
        scraping_success = asyncio.run(cricinfo_scrape_main())
        
        if scraping_success:
            logger.info("✅ Cricinfo scraping completed successfully!")
            
            # Trigger Cricinfo transformer
            transform_url = os.environ.get('TRANSFORM_CRICINFO_URL')
            if transform_url:
                logger.info("🔄 Triggering Cricinfo transformer...")
                response = requests.post(transform_url, json={}, timeout=600)
                if response.status_code == 200:
                    logger.info("✅ Cricinfo transformer triggered successfully!")
                    return {"status": "success", "message": "Cricinfo scraping and transformation completed"}
                else:
                    logger.error(f"❌ Failed to trigger Cricinfo transformer: {response.status_code}")
                    return {"status": "partial_success", "message": "Cricinfo scraping completed but transformer failed"}
            else:
                logger.warning("⚠️ TRANSFORM_CRICINFO_URL not set, skipping transformer trigger")
                return {"status": "success", "message": "Cricinfo scraping completed (no transformer trigger)"}
        else:
            logger.error("❌ Cricinfo scraping failed!")
            return {"status": "error", "message": "Cricinfo scraping failed"}
            
    except Exception as e:
        logger.error(f"❌ Cricinfo scraping pipeline error: {e}")
        return {"status": "error", "message": f"Cricinfo scraping pipeline error: {str(e)}"}

def transform_cricinfo(request):
    """
    Cloud Function entry point for Cricinfo data transformation.
    Triggered by scrape_cricinfo function or can be run independently.
    """
    try:
        logger.info("🔄 Starting Cricinfo data transformation...")
        
        # Import and run Cricinfo transformer
        from transform.cricinfo import main as cricinfo_transform_main
        
        logger.info("⚙️ Running Cricinfo transformer...")
        transform_success = cricinfo_transform_main()
        
        if transform_success:
            logger.info("✅ Cricinfo transformation completed successfully!")
            return {"status": "success", "message": "Cricinfo transformation completed"}
        else:
            logger.error("❌ Cricinfo transformation failed!")
            return {"status": "error", "message": "Cricinfo transformation failed"}
            
    except Exception as e:
        logger.error(f"❌ Cricinfo transformation error: {e}")
        return {"status": "error", "message": f"Cricinfo transformation error: {str(e)}"}

# For local testing
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        function_name = sys.argv[1]
        if function_name == "scrape_aucb":
            result = scrape_aucb(None)
        elif function_name == "transform_aucb":
            result = transform_aucb(None)
        elif function_name == "scrape_cricinfo":
            result = scrape_cricinfo(None)
        elif function_name == "transform_cricinfo":
            result = transform_cricinfo(None)
        else:
            print("Usage: python main.py [scrape_aucb|transform_aucb|scrape_cricinfo|transform_cricinfo]")
            sys.exit(1)
        
        print(f"Result: {result}")
    else:
        print("Usage: python main.py [scrape_aucb|transform_aucb|scrape_cricinfo|transform_cricinfo]")
