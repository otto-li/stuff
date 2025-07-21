import pandas as pd
import random
from datetime import datetime, timedelta
from mimesis import Person, Address, Internet, Datetime, Finance, Development, Numeric
from typing import Optional
import uuid

def generate_customer_website_traffic_data(num_rows: int = 1000, max_rows: int = 10000, accounts_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    Generate a realistic customer dataset with website traffic data.
    
    Args:
        num_rows (int): Number of rows to generate (default: 1000)
        max_rows (int): Maximum allowed rows (default: 10000)
        accounts_df (pd.DataFrame): Customer accounts dataset for email matching (optional)
    
    Returns:
        pd.DataFrame: Dataset with customer and website traffic information
    """
    # Ensure we don't exceed the maximum
    num_rows = min(num_rows, max_rows)
    
    # Initialize mimesis providers for realistic data generation
    person = Person()
    address = Address()
    internet = Internet()
    dt = Datetime()
    finance = Finance()
    development = Development()
    numeric = Numeric()
    
    # Define possible values for categorical data
    devices = ['Desktop', 'Mobile', 'Tablet']
    browsers = ['Chrome', 'Firefox', 'Safari', 'Edge', 'Opera']
    operating_systems = ['Windows', 'macOS', 'Android', 'iOS', 'Linux']
    referrers = ['Google', 'Facebook', 'Twitter', 'LinkedIn', 'Direct', 'Email', 'Other']
    page_categories = ['Home', 'Product', 'Category', 'Search', 'Cart', 'Checkout', 'Account', 'Help', 'Reviews', 'Sale']
    countries = ['Australia', 'Japan', 'Hong Kong', 'Singapore']
    
    # Generate data
    data = []
    
    # Create customer ID pool - mostly one-time visitors (guest checkout model)
    # Only 10% of customers will have multiple sessions (registered users)
    customer_pool_size = max(100, num_rows // 2)  # More unique customers for guest checkout model
    customer_ids = [str(uuid.uuid4()) for _ in range(customer_pool_size)]
    
    # Prepare account holder data for email matching if provided
    account_emails = []
    account_lookup = {}
    if accounts_df is not None and not accounts_df.empty:
        account_emails = accounts_df['email_address'].tolist()
        # Create lookup for account details
        for _, acc in accounts_df.iterrows():
            account_lookup[acc['email_address']] = {
                'customer_id': acc['customer_id'],
                'first_name': acc['first_name'],
                'last_name': acc['last_name'], 
                'country': acc['country'],
                'city': acc['city']
            }
        print(f"   Using {len(account_emails)} account emails for realistic matching...")
    
    for i in range(num_rows):
        # Customer information
        customer_id = random.choice(customer_ids)
        session_id = str(uuid.uuid4())
        
        # Generate session timestamp (last 90 days)
        # Create a more realistic date within the last 90 days
        base_date = datetime.now()
        days_ago = random.randint(1, 90)
        session_date = base_date - timedelta(days=days_ago)
        session_date = session_date.replace(hour=random.randint(0, 23), 
                                          minute=random.randint(0, 59),
                                          second=random.randint(0, 59))
        
        # Website traffic data - High traffic e-commerce patterns
        page_views = random.randint(1, 35)  # Higher page views for product browsing
        session_duration_minutes = round(random.uniform(0.3, 25.0), 2)
        
        # Generate realistic bounce rate based on page views (high for e-commerce)
        bounce_rate = 1.0 if page_views == 1 else 0.0
        
        # LOW conversion rate (realistic for e-commerce: 1-3%)
        # Base conversion rate around 2%
        base_conversion_rate = 0.02
        
        # Slight increase for longer sessions and more page views, but keep it low
        conversion_modifier = min((page_views * 0.001 + session_duration_minutes * 0.001), 0.015)
        conversion_probability = base_conversion_rate + conversion_modifier
        
        converted = random.random() < conversion_probability
        
        # Revenue (only if converted) - realistic e-commerce order values
        revenue = round(random.uniform(25.0, 350.0), 2) if converted else 0.0
        
        # Device and browser info
        device = random.choice(devices)
        browser = random.choice(browsers)
        os = random.choice(operating_systems)
        
        # Adjust OS based on device for realism
        if device == 'Mobile':
            os = random.choice(['Android', 'iOS'])
        elif device == 'Tablet':
            os = random.choice(['Android', 'iOS', 'Windows'])
        
        # Determine if this session should use an account email
        use_account_email = False
        selected_email = None
        
        if account_emails and len(account_emails) > 0:
            # Higher probability for converted sessions, especially non-guest
            if converted and random.random() < 0.7:  # 70% chance for conversions
                use_account_email = True
            elif not converted and random.random() < 0.15:  # 15% chance for browsing sessions
                use_account_email = True
            
            if use_account_email:
                selected_email = random.choice(account_emails)
        
        # Select country and generate appropriate city and timezone
        if use_account_email and selected_email in account_lookup:
            # Use account holder's location
            customer_country = account_lookup[selected_email]['country']
            customer_city = account_lookup[selected_email]['city']
        else:
            customer_country = random.choice(countries)
            
            # Define cities and timezones for each country
            country_data = {
                'Australia': {
                    'cities': ['Sydney', 'Melbourne', 'Brisbane', 'Perth', 'Adelaide', 'Canberra', 'Darwin', 'Hobart'],
                    'timezones': ['Australia/Sydney', 'Australia/Melbourne', 'Australia/Brisbane', 'Australia/Perth', 'Australia/Adelaide', 'Australia/Darwin']
                },
                'Japan': {
                    'cities': ['Tokyo', 'Osaka', 'Kyoto', 'Yokohama', 'Kobe', 'Nagoya', 'Sapporo', 'Fukuoka', 'Hiroshima'],
                    'timezones': ['Asia/Tokyo']
                },
                'Hong Kong': {
                    'cities': ['Central', 'Tsim Sha Tsui', 'Causeway Bay', 'Wan Chai', 'Mong Kok', 'Admiralty', 'Kowloon', 'Sha Tin'],
                    'timezones': ['Asia/Hong_Kong']
                },
                'Singapore': {
                    'cities': ['Singapore', 'Marina Bay', 'Orchard', 'Chinatown', 'Little India', 'Raffles Place', 'Sentosa'],
                    'timezones': ['Asia/Singapore']
                }
            }
            
            customer_city = random.choice(country_data[customer_country]['cities'])
        
        # Set timezone based on country
        country_timezones = {
            'Australia': ['Australia/Sydney', 'Australia/Melbourne', 'Australia/Brisbane', 'Australia/Perth', 'Australia/Adelaide', 'Australia/Darwin'],
            'Japan': ['Asia/Tokyo'],
            'Hong Kong': ['Asia/Hong_Kong'],
            'Singapore': ['Asia/Singapore']
        }
        customer_timezone = random.choice(country_timezones[customer_country])
        
        # Generate customer information (use account data if available)
        if use_account_email and selected_email in account_lookup:
            account_info = account_lookup[selected_email]
            customer_name = f"{account_info['first_name']} {account_info['last_name']}"
            customer_email = selected_email
            customer_id = account_info['customer_id']  # Use account's customer ID for consistency
        else:
            customer_name = person.full_name()
            customer_email = person.email()
            # Use existing customer_id from pool
        
        row = {
            # Customer Information
            'customer_id': customer_id,
            'customer_name': customer_name,
            'customer_email': customer_email,
            'customer_age': random.randint(18, 75),
            'customer_country': customer_country,
            'customer_city': customer_city,
            'registration_date': session_date - timedelta(days=random.randint(1, 730)),  # 1 day to 2 years before session
            
            # Session Information
            'session_id': session_id,
            'session_date': session_date,
            'session_start_time': session_date.strftime('%H:%M:%S'),
            
            # Website Traffic Data
            'page_views': page_views,
            'session_duration_minutes': session_duration_minutes,
            'bounce_rate': bounce_rate,
            'pages_per_session': round(page_views / 1.0, 2),
            'entry_page_category': random.choice(page_categories),
            'exit_page_category': random.choice(page_categories),
            
            # Technology Information
            'device_type': device,
            'browser': browser,
            'operating_system': os,
            'screen_resolution': f"{random.choice(['1920x1080', '1366x768', '1440x900', '1536x864', '375x667', '414x896'])}",
            
            # Traffic Source
            'referrer_source': random.choice(referrers),
            'campaign_source': f"campaign_{random.choice(['summer', 'winter', 'holiday', 'launch', 'promo'])}" if random.random() < 0.3 else None,
            'utm_medium': random.choice(['organic', 'cpc', 'email', 'social', 'direct']) if random.random() < 0.4 else None,
            
            # Behavioral Data
            'converted': converted,
            'revenue': revenue,
            'items_viewed': random.randint(0, min(page_views, 15)),  # Realistic product viewing
            'cart_abandonment': random.random() < 0.75 if page_views > 5 else False,  # High cart abandonment (75%)
            'newsletter_signup': random.random() < 0.03,  # Low newsletter signup rate (3%)
            'is_guest_checkout': converted and (random.random() < 0.2 if use_account_email else random.random() < 0.9) if converted else False,  # Account holders less likely to use guest checkout
            
            # Engagement Metrics
            'time_on_site_seconds': round(session_duration_minutes * 60, 1),
            'scroll_depth_percent': round(random.uniform(20, 100), 1),
            'click_through_rate': round(random.uniform(0.01, 0.15), 3),
            
            # Geographic Data
            'ip_address': internet.ip_v4(),
            'timezone': customer_timezone,
            'latitude': round(address.latitude(), 6),
            'longitude': round(address.longitude(), 6),
        }
        
        data.append(row)
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Add some derived columns
    df['customer_lifetime_value'] = df.groupby('customer_id')['revenue'].transform('sum')
    df['customer_session_count'] = df.groupby('customer_id').cumcount() + 1
    
    # Most customers are one-time visitors due to guest checkout model
    df['is_returning_customer'] = df['customer_session_count'] > 1
    df['has_account'] = ~df['is_guest_checkout'] & df['converted']  # Only non-guest purchasers have accounts
    df['days_since_registration'] = (df['session_date'] - df['registration_date']).dt.days
    
    # Adjust registration date for guest users (they don't really "register" until they buy without guest checkout)
    df.loc[df['is_guest_checkout'], 'registration_date'] = df.loc[df['is_guest_checkout'], 'session_date']
    
    # Sort by session date
    df = df.sort_values(['session_date', 'session_start_time']).reset_index(drop=True)
    
    return df

def save_dataset_to_csv(df: pd.DataFrame, filename: str = "customer_website_traffic_data.csv") -> None:
    """Save the generated dataset to a CSV file."""
    df.to_csv(filename, index=False)
    print(f"Dataset saved to {filename}")
    print(f"Dataset shape: {df.shape}")

def display_dataset_summary(df: pd.DataFrame) -> None:
    """Display a summary of the generated dataset."""
    print(f"\n{'='*60}")
    print("CUSTOMER WEBSITE TRAFFIC DATASET SUMMARY")
    print(f"{'='*60}")
    
    print(f"Total Records: {len(df):,}")
    print(f"Unique Customers: {df['customer_id'].nunique():,}")
    print(f"Unique Sessions: {df['session_id'].nunique():,}")
    print(f"Date Range: {df['session_date'].min().strftime('%Y-%m-%d')} to {df['session_date'].max().strftime('%Y-%m-%d')}")
    
    print(f"\nTraffic Metrics:")
    print(f"Average Page Views per Session: {df['page_views'].mean():.2f}")
    print(f"Average Session Duration: {df['session_duration_minutes'].mean():.2f} minutes")
    print(f"Bounce Rate: {(df['bounce_rate'].mean() * 100):.1f}%")
    
    print(f"\nE-commerce Metrics:")
    print(f"Conversion Rate: {(df['converted'].mean() * 100):.1f}%")
    print(f"Total Revenue: ${df['revenue'].sum():,.2f}")
    print(f"Average Order Value: ${df[df['converted']]['revenue'].mean():.2f}")
    print(f"Cart Abandonment Rate: {(df['cart_abandonment'].mean() * 100):.1f}%")
    print(f"Guest Checkout Rate: {(df[df['converted']]['is_guest_checkout'].mean() * 100):.1f}%")
    print(f"Account Creation Rate: {(df['has_account'].mean() * 100):.1f}%")
    print(f"Newsletter Signup Rate: {(df['newsletter_signup'].mean() * 100):.1f}%")
    
    print(f"\nDevice Distribution:")
    device_dist = df['device_type'].value_counts()
    for device, count in device_dist.items():
        print(f"  {device}: {count:,} ({count/len(df)*100:.1f}%)")
    
    print(f"\nTop 5 Traffic Sources:")
    referrer_dist = df['referrer_source'].value_counts().head()
    for source, count in referrer_dist.items():
        print(f"  {source}: {count:,} ({count/len(df)*100:.1f}%)")

def generate_customer_accounts_dataset(num_customers: int = 500, max_customers: int = 2000) -> pd.DataFrame:
    """
    Generate a dataset of known customer accounts that may match to the e-commerce dataset.
    These represent the ~10% of customers who create accounts instead of using guest checkout.
    
    Args:
        num_customers (int): Number of customer accounts to generate (default: 500)
        max_customers (int): Maximum allowed customers (default: 2000)
    
    Returns:
        pd.DataFrame: Dataset with customer account information
    """
    # Ensure we don't exceed the maximum
    num_customers = min(num_customers, max_customers)
    
    # Initialize mimesis providers
    person = Person()
    address = Address()
    internet = Internet()
    dt = Datetime()
    finance = Finance()
    
    # Define regional data
    countries = ['Australia', 'Japan', 'Hong Kong', 'Singapore']
    
    country_data = {
        'Australia': {
            'cities': ['Sydney', 'Melbourne', 'Brisbane', 'Perth', 'Adelaide', 'Canberra', 'Darwin', 'Hobart'],
            'timezones': ['Australia/Sydney', 'Australia/Melbourne', 'Australia/Brisbane', 'Australia/Perth', 'Australia/Adelaide', 'Australia/Darwin'],
            'phone_codes': ['+61']
        },
        'Japan': {
            'cities': ['Tokyo', 'Osaka', 'Kyoto', 'Yokohama', 'Kobe', 'Nagoya', 'Sapporo', 'Fukuoka', 'Hiroshima'],
            'timezones': ['Asia/Tokyo'],
            'phone_codes': ['+81']
        },
        'Hong Kong': {
            'cities': ['Central', 'Tsim Sha Tsui', 'Causeway Bay', 'Wan Chai', 'Mong Kok', 'Admiralty', 'Kowloon', 'Sha Tin'],
            'timezones': ['Asia/Hong_Kong'],
            'phone_codes': ['+852']
        },
        'Singapore': {
            'cities': ['Singapore', 'Marina Bay', 'Orchard', 'Chinatown', 'Little India', 'Raffles Place', 'Sentosa'],
            'timezones': ['Asia/Singapore'],
            'phone_codes': ['+65']
        }
    }
    
    # Customer segments and preferences
    customer_segments = ['VIP', 'Loyal', 'Regular', 'New', 'At Risk']
    marketing_preferences = ['Email', 'SMS', 'Phone', 'Mail', 'None']
    product_categories = ['Electronics', 'Fashion', 'Home', 'Sports', 'Beauty', 'Books', 'Toys', 'Garden']
    loyalty_tiers = ['Bronze', 'Silver', 'Gold', 'Platinum']
    
    data = []
    
    for i in range(num_customers):
        # Select country and generate regional data
        customer_country = random.choice(countries)
        country_info = country_data[customer_country]
        customer_city = random.choice(country_info['cities'])
        customer_timezone = random.choice(country_info['timezones'])
        phone_code = random.choice(country_info['phone_codes'])
        
        # Account creation date (last 3 years, with more recent signups)
        account_age_days = random.choices(
            [random.randint(1, 90), random.randint(91, 365), random.randint(366, 1095)],
            weights=[40, 35, 25]  # More recent accounts weighted higher
        )[0]
        account_created_date = datetime.now() - timedelta(days=account_age_days)
        
        # Customer demographics
        customer_age = random.randint(18, 70)
        gender = random.choice(['Male', 'Female', 'Other'])
        
        # Purchase history (for account holders)
        total_orders = random.randint(1, 25)  # Account holders have purchase history
        total_spent = round(random.uniform(50.0, 2500.0), 2)
        avg_order_value = round(total_spent / total_orders, 2) if total_orders > 0 else 0.0
        
        # Days since last purchase
        days_since_last_purchase = random.randint(1, 365)
        last_purchase_date = datetime.now() - timedelta(days=days_since_last_purchase)
        
        # Customer segment based on spending and recency
        if total_spent > 1500 and days_since_last_purchase < 30:
            segment = 'VIP'
            loyalty_tier = random.choice(['Gold', 'Platinum'])
        elif total_spent > 800 and days_since_last_purchase < 60:
            segment = 'Loyal'
            loyalty_tier = random.choice(['Silver', 'Gold'])
        elif days_since_last_purchase > 180:
            segment = 'At Risk'
            loyalty_tier = 'Bronze'
        elif total_orders <= 2:
            segment = 'New'
            loyalty_tier = 'Bronze'
        else:
            segment = 'Regular'
            loyalty_tier = random.choice(['Bronze', 'Silver'])
        
        # Communication preferences
        email_subscribed = random.random() < 0.7  # 70% subscribe to emails
        sms_subscribed = random.random() < 0.4   # 40% subscribe to SMS
        marketing_opt_in = random.random() < 0.6  # 60% opt into marketing
        
        # Preferred categories (1-3 categories per customer)
        num_preferred_categories = random.randint(1, 3)
        preferred_categories = random.sample(product_categories, num_preferred_categories)
        
        row = {
            # Basic Account Information
            'customer_id': str(uuid.uuid4()),
            'account_created_date': account_created_date,
            'account_status': random.choice(['Active', 'Active', 'Active', 'Active', 'Inactive']),  # 80% active
            
            # Personal Information
            'first_name': person.first_name(),
            'last_name': person.last_name(),
            'email_address': person.email(),
            'phone_number': f"{phone_code}-{random.randint(100000000, 999999999)}",
            'date_of_birth': dt.date(start=1954, end=2005),  # Age 18-70
            'gender': gender,
            
            # Address Information
            'country': customer_country,
            'city': customer_city,
            'postal_code': f"{random.randint(10000, 99999)}",
            'address_line_1': address.address(),
            'timezone': customer_timezone,
            
            # Purchase History
            'total_orders': total_orders,
            'total_lifetime_spend': total_spent,
            'average_order_value': avg_order_value,
            'first_purchase_date': account_created_date + timedelta(days=random.randint(0, 30)),
            'last_purchase_date': last_purchase_date,
            'days_since_last_purchase': days_since_last_purchase,
            
            # Customer Segmentation
            'customer_segment': segment,
            'loyalty_tier': loyalty_tier,
            'loyalty_points': random.randint(0, 5000) if loyalty_tier != 'Bronze' else random.randint(0, 500),
            
            # Preferences & Behavior
            'preferred_categories': ', '.join(preferred_categories),
            'email_subscribed': email_subscribed,
            'sms_subscribed': sms_subscribed,
            'marketing_opt_in': marketing_opt_in,
            'preferred_communication': random.choice(marketing_preferences),
            
            # Account Metrics
            'login_frequency_days': random.randint(1, 90),  # How often they log in
            'cart_save_count': random.randint(0, 5),  # Saved carts
            'wishlist_items': random.randint(0, 15),  # Items in wishlist
            'review_count': random.randint(0, min(total_orders, 10)),  # Reviews left
            'referral_count': random.randint(0, 3),  # Customers they referred
            
            # Risk & Fraud Indicators
            'payment_methods_count': random.randint(1, 4),
            'failed_payment_attempts': random.randint(0, 2),
            'return_rate_percent': round(random.uniform(0, 25), 1),  # % of orders returned
            'dispute_count': random.randint(0, 1),
            
            # Engagement Metrics
            'email_open_rate_percent': round(random.uniform(15, 85), 1) if email_subscribed else 0.0,
            'email_click_rate_percent': round(random.uniform(2, 15), 1) if email_subscribed else 0.0,
            'app_usage_days': random.randint(0, 30),  # Days used mobile app in last month
            'social_media_follower': random.random() < 0.3,  # 30% follow on social media
        }
        
        data.append(row)
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Add some derived columns
    df['account_age_days'] = (datetime.now() - df['account_created_date']).dt.days
    df['is_recent_customer'] = df['days_since_last_purchase'] <= 30
    df['is_high_value'] = df['total_lifetime_spend'] >= 1000
    df['is_frequent_buyer'] = df['total_orders'] >= 5
    df['customer_value_score'] = (
        (df['total_lifetime_spend'] / 100) * 0.4 +
        (df['total_orders']) * 0.3 +
        (30 - df['days_since_last_purchase'].clip(0, 30)) * 0.2 +
        (df['loyalty_points'] / 100) * 0.1
    ).round(2)
    
    # Sort by customer value score descending
    df = df.sort_values('customer_value_score', ascending=False).reset_index(drop=True)
    
    return df

def save_customer_accounts_to_csv(df: pd.DataFrame, filename: str = "customer_accounts_dataset.csv") -> None:
    """Save the customer accounts dataset to a CSV file."""
    df.to_csv(filename, index=False)
    print(f"Customer accounts dataset saved to {filename}")
    print(f"Dataset shape: {df.shape}")

def display_customer_accounts_summary(df: pd.DataFrame) -> None:
    """Display a summary of the customer accounts dataset."""
    print(f"\n{'='*60}")
    print("CUSTOMER ACCOUNTS DATASET SUMMARY")
    print(f"{'='*60}")
    
    print(f"Total Customer Accounts: {len(df):,}")
    print(f"Date Range: {df['account_created_date'].min().strftime('%Y-%m-%d')} to {df['account_created_date'].max().strftime('%Y-%m-%d')}")
    
    print(f"\nAccount Status:")
    status_dist = df['account_status'].value_counts()
    for status, count in status_dist.items():
        print(f"  {status}: {count:,} ({count/len(df)*100:.1f}%)")
    
    print(f"\nCustomer Segments:")
    segment_dist = df['customer_segment'].value_counts()
    for segment, count in segment_dist.items():
        print(f"  {segment}: {count:,} ({count/len(df)*100:.1f}%)")
    
    print(f"\nCountry Distribution:")
    country_dist = df['country'].value_counts()
    for country, count in country_dist.items():
        print(f"  {country}: {count:,} ({count/len(df)*100:.1f}%)")
    
    print(f"\nFinancial Metrics:")
    print(f"Total Lifetime Value: ${df['total_lifetime_spend'].sum():,.2f}")
    print(f"Average Lifetime Value: ${df['total_lifetime_spend'].mean():.2f}")
    print(f"Average Order Value: ${df['average_order_value'].mean():.2f}")
    print(f"High Value Customers (>$1000): {df['is_high_value'].sum():,} ({df['is_high_value'].mean()*100:.1f}%)")
    
    print(f"\nEngagement Metrics:")
    print(f"Email Subscription Rate: {df['email_subscribed'].mean()*100:.1f}%")
    print(f"Marketing Opt-in Rate: {df['marketing_opt_in'].mean()*100:.1f}%")
    print(f"Average Reviews per Customer: {df['review_count'].mean():.1f}")
    print(f"Recent Customers (30 days): {df['is_recent_customer'].sum():,} ({df['is_recent_customer'].mean()*100:.1f}%)")

def test_dataset_matching(session_df: pd.DataFrame, accounts_df: pd.DataFrame) -> dict:
    """
    Test matching between e-commerce session data and customer accounts data.
    
    Args:
        session_df: E-commerce session dataset
        accounts_df: Customer accounts dataset
    
    Returns:
        dict: Matching statistics and results
    """
    
    print(f"\n{'='*60}")
    print("DATASET MATCHING ANALYSIS")
    print(f"{'='*60}")
    
    # Initialize matching results
    matches = {
        'exact_email_matches': 0,
        'geographic_behavioral_matches': 0,
        'timing_pattern_matches': 0,
        'total_unique_matches': 0,
        'matched_session_ids': set(),
        'match_details': []
    }
    
    print(f"Starting datasets:")
    print(f"• Session records: {len(session_df):,}")
    print(f"• Account records: {len(accounts_df):,}")
    print(f"• Sessions with conversions: {len(session_df[session_df['converted']])}")
    print(f"• Non-guest conversions: {len(session_df[~session_df['is_guest_checkout'] & session_df['converted']])}")
    
    # 1. EXACT EMAIL MATCHING (Most reliable)
    print(f"\n1. EXACT EMAIL MATCHING:")
    session_emails = set(session_df['customer_email'].dropna())
    account_emails = set(accounts_df['email_address'].dropna())
    
    exact_matches = session_emails & account_emails
    matches['exact_email_matches'] = len(exact_matches)
    
    if exact_matches:
        matched_sessions = session_df[session_df['customer_email'].isin(exact_matches)]
        matches['matched_session_ids'].update(matched_sessions['session_id'])
        
        for email in exact_matches:
            session_info = session_df[session_df['customer_email'] == email].iloc[0]
            account_info = accounts_df[accounts_df['email_address'] == email].iloc[0]
            matches['match_details'].append({
                'type': 'exact_email',
                'email': email,
                'session_id': session_info['session_id'],
                'customer_id': session_info['customer_id'],
                'account_customer_id': account_info['customer_id'],
                'converted': session_info['converted'],
                'revenue': session_info['revenue'],
                'country': session_info['customer_country']
            })
    
    print(f"   Exact email matches: {matches['exact_email_matches']}")
    
    # 2. GEOGRAPHIC + BEHAVIORAL PATTERN MATCHING (Fuzzy matching)
    print(f"\n2. GEOGRAPHIC + BEHAVIORAL PATTERN MATCHING:")
    
    # Focus on converted sessions that haven't been matched yet
    unmatched_converted_sessions = session_df[
        (session_df['converted'] == True) & 
        (~session_df['session_id'].isin(matches['matched_session_ids']))
    ]
    
    geographic_behavioral_matches = 0
    
    for _, session in unmatched_converted_sessions.iterrows():
        # Look for accounts in same country with similar spending patterns
        potential_matches = accounts_df[
            (accounts_df['country'] == session['customer_country']) &
            (accounts_df['account_status'] == 'Active')
        ]
        
        if len(potential_matches) > 0:
            # Find accounts with similar order values
            revenue_tolerance = 50  # $50 tolerance
            order_value_matches = potential_matches[
                abs(potential_matches['average_order_value'] - session['revenue']) <= revenue_tolerance
            ]
            
            if len(order_value_matches) > 0:
                # Additional criteria: timing (account created before session)
                session_date = pd.to_datetime(session['session_date'])
                timing_matches = order_value_matches[
                    pd.to_datetime(order_value_matches['account_created_date']) <= session_date
                ]
                
                if len(timing_matches) > 0:
                    # Take the best match (closest order value)
                    best_match = timing_matches.iloc[
                        abs(timing_matches['average_order_value'] - session['revenue']).argmin()
                    ]
                    
                    # Avoid double-matching the same account
                    if best_match['customer_id'] not in [m['account_customer_id'] for m in matches['match_details']]:
                        matches['matched_session_ids'].add(session['session_id'])
                        geographic_behavioral_matches += 1
                        
                        matches['match_details'].append({
                            'type': 'geographic_behavioral',
                            'session_id': session['session_id'],
                            'customer_id': session['customer_id'],
                            'account_customer_id': best_match['customer_id'],
                            'converted': session['converted'],
                            'revenue': session['revenue'],
                            'account_avg_order': best_match['average_order_value'],
                            'country': session['customer_country'],
                            'match_confidence': 'medium'
                        })
    
    matches['geographic_behavioral_matches'] = geographic_behavioral_matches
    print(f"   Geographic + behavioral matches: {geographic_behavioral_matches}")
    
    # 3. TIMING PATTERN MATCHING (For recent account activity)
    print(f"\n3. TIMING PATTERN MATCHING:")
    
    timing_matches = 0
    
    # Look for sessions around the time of recent account activity
    for _, account in accounts_df.iterrows():
        if account['days_since_last_purchase'] <= 7:  # Recent activity within 7 days
            
            account_last_purchase = datetime.now() - timedelta(days=account['days_since_last_purchase'])
            
            # Find sessions in same country within 3 days of last purchase
            nearby_sessions = session_df[
                (session_df['customer_country'] == account['country']) &
                (abs((pd.to_datetime(session_df['session_date']) - account_last_purchase).dt.days) <= 3) &
                (~session_df['session_id'].isin(matches['matched_session_ids']))
            ]
            
            if len(nearby_sessions) > 0:
                # Take the closest timing match
                best_session = nearby_sessions.iloc[
                    abs((pd.to_datetime(nearby_sessions['session_date']) - account_last_purchase).dt.days).argmin()
                ]
                
                matches['matched_session_ids'].add(best_session['session_id'])
                timing_matches += 1
                
                matches['match_details'].append({
                    'type': 'timing_pattern',
                    'session_id': best_session['session_id'],
                    'customer_id': best_session['customer_id'],
                    'account_customer_id': account['customer_id'],
                    'converted': best_session['converted'],
                    'revenue': best_session['revenue'],
                    'country': best_session['customer_country'],
                    'days_difference': abs((pd.to_datetime(best_session['session_date']) - account_last_purchase).days),
                    'match_confidence': 'low'
                })
    
    matches['timing_pattern_matches'] = timing_matches
    print(f"   Timing pattern matches: {timing_matches}")
    
    # Calculate total unique matches
    matches['total_unique_matches'] = len(matches['matched_session_ids'])
    
    # Calculate match rate as percentage of e-commerce dataset
    match_rate_percent = (matches['total_unique_matches'] / len(session_df)) * 100
    conversion_match_rate = (matches['total_unique_matches'] / len(session_df[session_df['converted']])) * 100 if len(session_df[session_df['converted']]) > 0 else 0
    
    print(f"\n{'='*60}")
    print("MATCHING RESULTS SUMMARY")
    print(f"{'='*60}")
    
    print(f"Total matches found: {matches['total_unique_matches']:,}")
    print(f"Match rate (% of all sessions): {match_rate_percent:.2f}%")
    print(f"Match rate (% of converted sessions): {conversion_match_rate:.2f}%")
    
    print(f"\nBreakdown by matching method:")
    print(f"• Exact email matches: {matches['exact_email_matches']:,}")
    print(f"• Geographic + behavioral: {matches['geographic_behavioral_matches']:,}")
    print(f"• Timing patterns: {matches['timing_pattern_matches']:,}")
    
    # Show sample matches
    if matches['match_details']:
        print(f"\nSample matches (first 3):")
        for i, match in enumerate(matches['match_details'][:3]):
            print(f"  {i+1}. {match['type'].title()} Match:")
            print(f"     Session ID: {match['session_id']}")
            print(f"     Country: {match['country']}")
            print(f"     Converted: {match['converted']}, Revenue: ${match.get('revenue', 0):.2f}")
            if 'match_confidence' in match:
                print(f"     Confidence: {match['match_confidence']}")
    
    # Calculate business impact
    matched_revenue = sum([m['revenue'] for m in matches['match_details'] if m['converted']])
    total_revenue = session_df['revenue'].sum()
    revenue_coverage = (matched_revenue / total_revenue * 100) if total_revenue > 0 else 0
    
    print(f"\nBusiness Impact:")
    print(f"• Revenue from matched sessions: ${matched_revenue:,.2f}")
    print(f"• Total e-commerce revenue: ${total_revenue:,.2f}")
    print(f"• Revenue coverage by matches: {revenue_coverage:.1f}%")
    
    matches['match_rate_percent'] = match_rate_percent
    matches['conversion_match_rate'] = conversion_match_rate
    matches['revenue_coverage'] = revenue_coverage
    
    return matches

def main():
    """Generate and display both e-commerce session data and customer accounts data."""
    print("🛒 GENERATING E-COMMERCE DATASETS")
    print("="*60)
    
    # Generate customer accounts dataset FIRST (so we can use emails for matching)
    print("\n1. Generating customer accounts dataset...")
    # Generate fewer accounts since only ~10% of customers create accounts
    accounts_dataset = generate_customer_accounts_dataset(num_customers=100)
    
    # Display accounts dataset summary
    display_customer_accounts_summary(accounts_dataset)
    
    # Save accounts dataset to CSV
    save_customer_accounts_to_csv(accounts_dataset)
    
    print("\n" + "="*60)
    
    # Generate e-commerce session dataset (using account emails for realistic matching)
    print("\n2. Generating customer website traffic dataset with email matching...")
    session_dataset = generate_customer_website_traffic_data(num_rows=1000, accounts_df=accounts_dataset)
    
    # Display session dataset summary
    display_dataset_summary(session_dataset)
    
    # Save session dataset to CSV
    save_dataset_to_csv(session_dataset)
    
    # Show sample data from both datasets
    print(f"\n" + "="*80)
    print("SAMPLE DATA COMPARISON")
    print("="*80)
    
    print(f"\nSESSION DATA (first 3 rows - key columns):")
    ecommerce_cols = ['customer_country', 'customer_city', 'page_views', 'converted', 'revenue', 
                     'is_guest_checkout', 'cart_abandonment', 'device_type']
    print(session_dataset.head(3)[ecommerce_cols].to_string(index=False))
    
    print(f"\nCUSTOMER ACCOUNTS DATA (first 3 rows - key columns):")
    account_cols = ['country', 'city', 'customer_segment', 'total_orders', 'total_lifetime_spend', 
                   'loyalty_tier', 'email_subscribed', 'days_since_last_purchase']
    print(accounts_dataset.head(3)[account_cols].to_string(index=False))
    
    # Show potential matching insights
    print(f"\n" + "="*60)
    print("DATASET MATCHING POTENTIAL")
    print("="*60)
    
    session_countries = set(session_dataset['customer_country'].unique())
    account_countries = set(accounts_dataset['country'].unique())
    
    print(f"Session data countries: {sorted(session_countries)}")
    print(f"Account data countries: {sorted(account_countries)}")
    print(f"Overlapping countries: {sorted(session_countries & account_countries)}")
    
    # Conversion insights
    converted_sessions = session_dataset[session_dataset['converted'] == True]
    non_guest_sessions = converted_sessions[converted_sessions['is_guest_checkout'] == False]
    
    print(f"\nMatching potential insights:")
    print(f"• Total session records: {len(session_dataset):,}")
    print(f"• Total account records: {len(accounts_dataset):,}")
    print(f"• Sessions with conversions: {len(converted_sessions):,}")
    print(f"• Non-guest conversions (potential account matches): {len(non_guest_sessions):,}")
    print(f"• Account holders could match to ~{len(non_guest_sessions)} sessions")
    
    # Test dataset matching
    print(f"\n" + "="*60)
    print("TESTING DATASET MATCHING")
    print("="*60)
    test_dataset_matching(session_dataset, accounts_dataset)
    
    print(f"\n📊 Both datasets saved and ready for analysis!")
    print(f"💡 Use email addresses, location, or customer behavior patterns for matching.")


if __name__ == "__main__":
    main()
