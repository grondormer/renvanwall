import base58
import json
import os
import time
import threading
from solders.keypair import Keypair
from github import Github, Auth
import signal
from dotenv import load_dotenv
from flask import Flask, Response, request, jsonify
import requests

# Load environment variables from .env file
load_dotenv()

# Disable SSL warnings for GitHub API
import urllib3
urllib3.disable_warnings()

# Global flag for clean shutdown
shutdown_flag = False
app = Flask(__name__)
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0  # Disable caching for development

# Store found wallets
found_wallets = []
found_wallets_lock = threading.Lock()

# Global variables for wallet generation (using threading instead of multiprocessing for gunicorn compatibility)
wallet_counter = 0
counter_lock = threading.Lock()

# Keep-alive status
keep_alive_active = False  # Start disabled - use web interface to enable
keep_alive_thread = None
keep_alive_stop = threading.Event()

def signal_handler(sig, frame):
    global shutdown_flag
    shutdown_flag = True
    print("\n👋 Shutting down gracefully...")

# Configuration
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
REPO_NAME = os.getenv('REPO_NAME', 'bonk-wallets')
FILE_NAME = "wallets.json"  # File to store wallets in the repo
SOLANA_RPC_URL = os.getenv('SOLANA_RPC_URL', 'https://api.mainnet-beta.solana.com')
MIN_BALANCE = 0  # Save wallets with any balance > 0
BATCH_SIZE = 10000  # Generate 10,000 wallets per batch
# KEEP_ALIVE_ENABLED is no longer used - keep-alive is controlled via web interface

def check_wallet_balance(public_key):
    """Check the balance of a Solana wallet using RPC."""
    try:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBalance",
            "params": [public_key]
        }
        
        response = requests.post(
            SOLANA_RPC_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=5  # Reduced timeout to 5 seconds
        )
        
        if response.status_code == 200:
            data = response.json()
            if 'result' in data and 'value' in data['result']:
                # Balance is returned in lamports, convert to SOL (1 SOL = 1,000,000,000 lamports)
                balance_lamports = data['result']['value']
                balance_sol = balance_lamports / 1_000_000_000
                return balance_sol
        return 0
    except Exception as e:
        # Silently ignore errors to avoid spamming the console
        return 0

def check_wallet_balances_batch(wallets, max_workers=20):
    """Check balances for multiple wallets in parallel using threading."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    results = {}
    checked_count = 0
    total_wallets = len(wallets)
    
    def check_single(wallet):
        public_key = wallet['public_key']
        balance = check_wallet_balance(public_key)
        return public_key, balance, wallet['private_key']
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all balance checks
        future_to_wallet = {executor.submit(check_single, wallet): wallet for wallet in wallets}
        
        # Collect results as they complete
        for future in as_completed(future_to_wallet):
            public_key, balance, private_key = future.result()
            results[public_key] = balance
            checked_count += 1
            
            # Print full wallet information for every wallet
            print(f"  [{checked_count}/{total_wallets}]")
            print(f"    Public Key:  {public_key}")
            print(f"    Private Key: {private_key}")
            print(f"    Balance:     {balance} SOL", flush=True)
    
    return results

class WalletGenerator:
    def __init__(self):
        self.github = None
        self.repo = None
        self.wallets = set()
        self.existing_wallets = []
        self.last_save = 0
        self.save_interval = 60  # Save to GitHub every 60 seconds
        self.initial_github_check_done = False
        self.start_time = time.time()
        self.last_print = self.start_time
        self.last_count = 0
        self.print_interval = 10.0  # Print status every 10 seconds (batch processing takes longer)
        self.running = False  # Track if the generator is currently running

    def print_status(self, force=False):
        current_time = time.time()
        time_since_last_print = current_time - self.last_print

        # Safely get the current count with lock if available
        if counter_lock is not None:
            with counter_lock:
                current_count = wallet_counter
        else:
            current_count = 0
        
        # Calculate rate based on actual change since last print
        count_diff = current_count - self.last_count
        elapsed = current_time - self.start_time
        rate = (count_diff / time_since_last_print) if time_since_last_print > 0 else 0
        
        # Print status
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        print(f"[{timestamp}] 🔍 Total: {current_count:,} wallets | "
              f"Rate: {rate:,.0f} w/s | "
              f"Found: {len(found_wallets):,}", flush=True)
        
        # Update tracking variables
        self.last_count = current_count
        self.last_print = current_time

    def setup_github(self, test_only=False):
        """Initialize GitHub connection and get the repository.
        
        Args:
            test_only (bool): If True, only test the connection without loading wallets
        """
        if not GITHUB_TOKEN or GITHUB_TOKEN == "your_github_token_here":
            print("❌ GitHub token not set or using default value.")
            return False
            
        if len(GITHUB_TOKEN) < 40:  # GitHub tokens are usually 40+ characters
            print("❌ Invalid GitHub token: Token appears to be too short")
            return False
            
        try:
            if not self.github:
                print("🔑 Attempting to authenticate with GitHub...")
                auth = Auth.Token(GITHUB_TOKEN)
                self.github = Github(auth=auth)
                
                # Test the connection
                user = self.github.get_user()
                print(f"✅ Authenticated as GitHub user: {user.login}")
                
                # Get the repo
                print(f"🔍 Attempting to access repository: {REPO_NAME}")
                self.repo = self.github.get_repo(REPO_NAME)
                print(f"✅ Successfully connected to repository: {REPO_NAME}")
                
                if not test_only:
                    try:
                        contents = self.repo.get_contents(FILE_NAME)
                        print(f"📁 Found existing {FILE_NAME} in repository")
                    except:
                        print(f"ℹ️ {FILE_NAME} not found in repository. It will be created when the first wallet is found.")
            return True
                        
        except Exception as e:
            print(f"❌ Error connecting to GitHub: {str(e)}")
            if not test_only:
                print("Please verify:")
                print("1. Your GitHub token is correct and has 'repo' scope")
                print(f"2. The repository '{REPO_NAME}' exists and is accessible")
                print("3. The token has the correct permissions")
            self.repo = None
            return False

    def load_existing_wallets(self):
        """Load existing wallets from GitHub or create an empty list."""
        self.existing_wallets = []
        if self.repo:
            try:
                content = self.repo.get_contents(FILE_NAME)
                existing_data = json.loads(content.decoded_content.decode())
                if isinstance(existing_data, list):
                    self.existing_wallets = existing_data
                    print(f"Loaded {len(self.existing_wallets)} existing wallets")
                else:
                    print("Existing wallet data is not in the expected format, starting fresh")
            except Exception as e:
                print(f"No existing wallets file found or error loading: {str(e)}")
                print("A new wallets file will be created when the first wallet is found")

    def save_wallet(self, wallet_data):
        """Save wallet data to GitHub repository."""
        if not self.repo:
            print("GitHub repository not available. Wallet not saved to GitHub.")
            return

        try:
            try:
                # Try to get the existing file
                contents = self.repo.get_contents(FILE_NAME)
                # Get existing wallets
                existing_wallets = json.loads(contents.decoded_content.decode())
                if not isinstance(existing_wallets, list):
                    existing_wallets = []
                
                # Check if wallet already exists to avoid duplicates
                wallet_exists = any(
                    w['public_key'] == wallet_data['public_key'] 
                    for w in existing_wallets
                )
                
                if not wallet_exists:
                    # Add the new wallet with balance
                    existing_wallets.append({
                        'public_key': wallet_data['public_key'],
                        'private_key': wallet_data['private_key'],
                        'balance': wallet_data.get('balance', 0)
                    })
                    
                    # Update the file with all wallets
                    self.repo.update_file(
                        FILE_NAME,
                        f"Add wallet with balance: {wallet_data['public_key']} ({wallet_data.get('balance', 0)} SOL)",
                        json.dumps(existing_wallets, indent=2),
                        contents.sha
                    )
                    print(f"✅ Saved wallet to GitHub: {wallet_data['public_key']} (Balance: {wallet_data.get('balance', 0)} SOL)")
                else:
                    print(f"ℹ️ Wallet already exists in GitHub: {wallet_data['public_key']}")
                    
            except Exception as e:
                # File doesn't exist or other error, create new file with this wallet
                if 'Not Found' in str(e):
                    self.repo.create_file(
                        FILE_NAME,
                        "Initial commit: Add first wallet with balance",
                        json.dumps([{
                            'public_key': wallet_data['public_key'],
                            'private_key': wallet_data['private_key'],
                            'balance': wallet_data.get('balance', 0)
                        }], indent=2)
                    )
                    print(f"✅ Created new file and saved wallet to GitHub: {wallet_data['public_key']} (Balance: {wallet_data.get('balance', 0)} SOL)")
                else:
                    raise
                    
        except Exception as e:
            print(f"❌ Error saving to GitHub: {str(e)}")
            print("Make sure your GitHub token has write access to the repository.")

    @staticmethod
    def generate_wallet_batch(batch_size=BATCH_SIZE):
        """Generate a batch of random Solana keypairs."""
        batch = []
        # Pre-allocate list for better performance
        batch = [None] * batch_size
        for i in range(batch_size):
            keypair = Keypair()
            pubkey = str(keypair.pubkey())
            batch[i] = ({
                'public_key': pubkey,
                'private_key': base58.b58encode(bytes(keypair)).decode('utf-8')
            })
        return batch

    def run(self):
        """Main wallet generation loop with balance checking."""
        global wallet_counter
        self.running = True
        print(f"🚀 Starting to generate random wallets and check balances...")
        print(f"📊 Batch size: {BATCH_SIZE:,} wallets per batch")
        print(f"📊 Minimum balance threshold: {MIN_BALANCE} SOL")
        print(f"🌐 Using RPC endpoint: {SOLANA_RPC_URL}")

        # Reset counter when starting if counter_lock is available
        if counter_lock is not None:
            with counter_lock:
                wallet_counter = 0

        # Initial GitHub connection test (moved outside the main loop)
        if not self.setup_github(test_only=True):
            print("⚠️ GitHub connection test failed. Wallets will be generated but not saved to GitHub.")
        else:
            print("✅ GitHub connection test successful. Will save wallets with balances when found.")
            self.initial_github_check_done = True

        # Reset timers
        self.start_time = time.time()
        self.last_print = self.start_time
        self.last_count = 0

        print("🔄 Running wallet generation in main thread (single-threaded for gunicorn compatibility)")

        try:
            # Simple loop for generating batches - no multiprocessing
            batch_count = 0
            while not shutdown_flag:
                batch_count += 1

                # Generate a batch of wallets
                batch = WalletGenerator.generate_wallet_batch(BATCH_SIZE)

                # Update counter
                if counter_lock is not None:
                    with counter_lock:
                        wallet_counter += len(batch)

                # Check balances for all wallets in parallel using threading
                print(f"🔄 Checking balances for {len(batch)} wallets (batch {batch_count})...")
                balance_results = check_wallet_balances_batch(batch, max_workers=50)

                # Process results
                for wallet in batch:
                    public_key = wallet['public_key']
                    balance = balance_results.get(public_key, 0)
                    if balance > MIN_BALANCE:
                        wallet['balance'] = balance

                        # Add to found wallets
                        with found_wallets_lock:
                            found_wallets.append(wallet)

                        # Print match
                        print(f"\n🎉 Found wallet with balance: {public_key} ({balance} SOL)")

                        # Save to GitHub in a separate thread
                        if self.initial_github_check_done:
                            try:
                                threading.Thread(
                                    target=self.save_wallet,
                                    args=(wallet,),
                                    daemon=True
                                ).start()
                            except Exception as e:
                                print(f"❌ Error queuing save: {str(e)}")

                # Print status
                current_time = time.time()
                if current_time - self.last_print >= self.print_interval:
                    self.print_status()

                if shutdown_flag:
                    print("\n👋 Shutting down...")
                    break

        except Exception as e:
            print(f"\n❌ Error: {str(e)}")
        finally:
            # Final status update
            with counter_lock:
                current_count = wallet_counter
                elapsed = time.time() - self.start_time
                rate = current_count / elapsed if elapsed > 0 else 0
                
                timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
                print(f"\n✨ Final Stats:")
                print(f"   Total Wallets Checked: {current_count:,}")
                print(f"   Total with Balance: {len(found_wallets):,}")
                print(f"   Total Runtime: {elapsed:.2f} seconds")
                print(f"   Average Rate: {rate:,.2f} wallets/second")
            
            print("\n✨ Wallet generation stopped!")

    def start(self):
        """Start the wallet generation in a separate thread."""
        if self.running:
            return False
        
        self.running = True
        self.worker_thread = threading.Thread(target=self.run, daemon=True)
        self.worker_thread.start()
        return True
    
    def stop(self):
        """Stop the wallet generation."""
        global shutdown_flag
        if self.running:
            shutdown_flag = True
            if hasattr(self, 'worker_thread') and self.worker_thread is not None:
                self.worker_thread.join(timeout=5)
            self.running = False
            shutdown_flag = False
            return True
        return False
    
    def get_status(self):
        """Get current status of wallet generation."""
        if counter_lock is not None:
            with counter_lock:
                count = wallet_counter
                elapsed = time.time() - self.start_time
                rate = count / elapsed if elapsed > 0 else 0
        else:
            count = 0
            elapsed = 0
            rate = 0
            
        return {
            'is_running': self.running,
            'wallets_checked': count,
            'wallets_found': len(found_wallets),
            'elapsed_time': elapsed,
            'rate_per_second': rate
        }

# Initialize the wallet generator and start generation in a background thread
generator = WalletGenerator()

def keep_alive_worker():
    """Background thread function to ping the site."""
    global keep_alive_active
    import requests
    from datetime import datetime
    
    while not keep_alive_stop.is_set():
        try:
            # Default to the Render site if not specified
            site_url = os.getenv('RENDER_SITE_URL', 'https://newagers.onrender.com')
            
            # Ensure proper URL format
            if not site_url.startswith(('http://', 'https://')):
                site_url = f'https://{site_url}'
            site_url = site_url.rstrip('/')
            
            # Make the request with a user agent and timeout
            headers = {'User-Agent': 'BonkVanityKeepAlive/1.0'}
            start_time = time.time()
            response = requests.get(
                site_url, 
                timeout=10, 
                headers=headers,
                verify=True  # Verify SSL certificate
            )
            response_time = (time.time() - start_time) * 1000  # in milliseconds
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Pinged {site_url} - Status: {response.status_code} ({response_time:.2f}ms)")
        except requests.exceptions.SSLError as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] SSL Error: {str(e)}")
            # Wait a bit longer on SSL errors to avoid hammering
            keep_alive_stop.wait(300)  # 5 minutes
            continue
        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Error pinging site: {str(e)}")
        
        # Wait for 1 minute or until stop is requested
        keep_alive_stop.wait(60)
    
    keep_alive_active = False
    print("Keep-alive worker stopped")

# Keep-alive starts disabled - use web interface to control it
print("ℹ️ Keep-alive disabled by default - use web interface to enable")

def start_keep_alive():
    """Start the keep-alive background thread."""
    global keep_alive_active, keep_alive_thread, keep_alive_stop
    
    if not keep_alive_active:
        keep_alive_stop.clear()
        keep_alive_thread = threading.Thread(target=keep_alive_worker, daemon=True)
        keep_alive_thread.start()
        keep_alive_active = True
        return True
    return False

def stop_keep_alive():
    """Stop the keep-alive background thread."""
    global keep_alive_active, keep_alive_thread, keep_alive_stop
    
    if keep_alive_active:
        keep_alive_stop.set()
        if keep_alive_thread:
            keep_alive_thread.join(timeout=2.0)
        keep_alive_thread = None
        keep_alive_active = False
        return True
    return False

@app.route('/')
def index():
    """Serve the main page with wallet generation stats and keep-alive controls."""
    global wallet_counter, keep_alive_active
    with found_wallets_lock:
        # Get the current stats
        
        # Safely get the total generated count if counter_lock is available
        if counter_lock is not None:
            with counter_lock:
                total_generated = wallet_counter
        else:
            total_generated = 0
            
        total_matched = len(found_wallets)
        
        # Generate HTML response
        # Prepare the dynamic parts of the HTML
        active_class = ' active' if keep_alive_active else ''
        # Invert the button text since keep-alive starts active by default
        button_text = 'Start Keep-Alive' if not keep_alive_active else 'Stop Keep-Alive'
        
        html = """<!DOCTYPE html>
        <html>
        <head>
            <title>Solana Wallet Balance Checker</title>
            <style>
                body {{ font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }}
                .stats {{ background: #f5f5f5; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
                .control-panel {{ background: #e9f7fe; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
                button {{ 
                    background: #4CAF50; 
                    color: white; 
                    border: none; 
                    padding: 10px 20px; 
                    text-align: center; 
                    text-decoration: none; 
                    display: inline-block; 
                    font-size: 16px; 
                    margin: 4px 2px; 
                    cursor: pointer; 
                    border-radius: 4px;
                }}
                button:disabled {{ background: #cccccc; cursor: not-allowed; }}
                #status {{ font-weight: bold; }}
                .active {{ color: #4CAF50; }}
                .inactive {{ color: #f44336; }}
                .keep-alive-btn {{
                    position: fixed;
                    bottom: 20px;
                    right: 20px;
                    z-index: 1000;
                    background: #4CAF50;
                    color: white;
                    border: none;
                    padding: 10px 20px;
                    border-radius: 5px;
                    cursor: pointer;
                }}
                .keep-alive-btn:disabled {{
                    background: #cccccc;
                    cursor: not-allowed;
                }}
                .keep-alive-btn.active {{
                    background: #f44336;
                }}
            </style>
        </head>
        <body>
            <h1>Solana Wallet Balance Checker</h1>
            
            <div class="stats">
                <h2>Wallet Generation Stats</h2>
                <p>Total Wallets Checked: {total_generated:,}</p>
                <p>Wallets with Balance Found: {total_matched:,}</p>
                <p>Last updated: {current_time}</p>
            </div>
            
            <button id="keepAliveBtn" class="keep-alive-btn{active_class}" 
                    onclick="toggleKeepAlive()">
                {button_text}
            </button>
            
            <script>
                function toggleKeepAlive() {{
                    const btn = document.getElementById('keepAliveBtn');
                    const isStarting = btn.textContent.trim() === 'Start Keep-Alive';
                    btn.disabled = true;
                    
                    fetch(isStarting ? '/start-keepalive' : '/stop-keepalive', {{
                        method: 'POST',
                        headers: {{
                            'Content-Type': 'application/json',
                        }}
                    }})
                    .then(response => response.json())
                    .then(data => {{
                        if (data.success) {{
                            btn.textContent = data.active ? 'Stop Keep-Alive' : 'Start Keep-Alive';
                            if (data.active) {{
                                btn.classList.add('active');
                            }} else {{
                                btn.classList.remove('active');
                            }}
                            console.log(data.message);
                        }} else {{
                            console.error('Error:', data.message);
                        }}
                    }})
                    .catch(error => {{
                        console.error('Error:', error);
                    }})
                    .finally(() => {{
                        btn.disabled = false;
                    }});
                }}
            </script>
        </body>
        </html>""".format(
            total_generated=total_generated,
            total_matched=total_matched,
            current_time=time.strftime('%Y-%m-%d %H:%M:%S'),
            active_class=active_class,
            button_text=button_text
        )
        
        return Response(html, mimetype='text/html')

@app.route('/start-keepalive', methods=['POST'])
def start_keepalive():
    """Start the keep-alive ping."""
    global keep_alive_active, keep_alive_thread, keep_alive_stop
    
    if not keep_alive_active:
        keep_alive_stop.clear()  # Clear the existing event to allow the worker to run
        keep_alive_thread = threading.Thread(
            target=keep_alive_worker,
            daemon=True
        )
        keep_alive_thread.start()
        keep_alive_active = True
        print("✅ Keep-alive started")
    
    return jsonify({
        'success': True,
        'active': keep_alive_active,
        'message': 'Keep-alive started' if keep_alive_active else 'Keep-alive already running'
    })

@app.route('/stop-keepalive', methods=['POST'])
def stop_keepalive():
    """Stop the keep-alive ping."""
    global keep_alive_active, keep_alive_stop, keep_alive_thread
    
    if keep_alive_active:
        keep_alive_stop.set()
        if keep_alive_thread:
            keep_alive_thread.join(timeout=2.0)
        keep_alive_thread = None
        keep_alive_active = False
        print("🛑 Keep-alive stopped")
    
    return jsonify({
        'success': True,
        'active': keep_alive_active,
        'message': 'Keep-alive stopped' if not keep_alive_active else 'Failed to stop keep-alive'
    })

# Start wallet generation in a background thread
def start_wallet_generation():
    return generator.start()

# Schedule auto-start after 1 minute
# Check for GitHub token
if not GITHUB_TOKEN:
    print("⚠️ Please set your GITHUB_TOKEN in the .env file!")

# Start wallet generation in a background thread
wallet_thread = threading.Thread(target=start_wallet_generation, daemon=True)
wallet_thread.start()

print("✅ Wallet generation initialized and started")

# Start the wallet generation when the script runs
if __name__ == "__main__":
    # Register signal handler for clean shutdown
    def signal_handler(sig, frame):
        global shutdown_flag
        print("\n👋 Shutting down gracefully...")
        shutdown_flag = True
        stop_keep_alive()
        wallet_thread.join(timeout=5)
        print("Cleanup complete. Goodbye!")
        os._exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    # Keep-alive control endpoints are defined above the main block

    # Start the Flask app
    port = int(os.getenv('PORT', 5000))
    print(f"[Web] Server running on http://localhost:{port}")
    print("[Web] Access the web interface to control the keep-alive functionality")
    print("[Web] The wallet balance checker is running in the background")
    print("[Web] Press Ctrl+C to stop")
    
    try:
        app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
    except KeyboardInterrupt:
        print("\n👋 Keyboard interrupt received. Shutting down...")
        shutdown_flag = True
        stop_keep_alive()
        wallet_thread.join(timeout=5)
        print("Cleanup complete. Goodbye!")
    finally:
        # Ensure keep-alive is stopped
        stop_keep_alive()
