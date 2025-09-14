# Snowball Effect - Xero Aged Receivables Report System

## Overview

This system consolidates aged receivables reports from Xero for 76 companies under a unified platform. It provides automated report generation with advanced caching, queuing, and multi-app support for efficient processing of large-scale financial data.

## System Architecture

### Core Components

1. **API Layer** (`api.py`) - FastAPI endpoints for report generation
2. **Service Layer** (`service.py`) - Core business logic for data processing
3. **Helper Functions** (`helper.py`) - Utility functions for calculations and formatting
4. **Export Module** (`export.py`) - Excel report generation and formatting

## 1. Queuing System

### 1.1 Worker Thread Architecture

The system implements a sophisticated queuing mechanism to handle report generation requests efficiently:

- **Background Processing**: Reports are processed asynchronously using worker threads
- **Database Queue**: Uses `DatabaseQueueService` to manage job queues
- **Job Management**: Each report request is assigned a unique `job_id` for tracking
- **Status Monitoring**: Real-time status updates for queued jobs

**Key Features:**
- Prevents server timeouts during long-running operations (4-5 minutes per report)
- Handles multiple concurrent requests without blocking
- Automatic retry mechanisms for failed jobs
- Job prioritization and resource management

**Implementation:**
```python
# Queue a job for background processing
queue_service = DatabaseQueueService(db)
job_id = queue_service.enqueue_job("aged_receivables_report", job_data)
```

## 2. Caching System

### Redis-Based Caching

The system uses Redis for intelligent caching to optimize performance:

**Cache Strategy:**
- **Multi-level Caching**: Different cache keys for different data types
- **TTL Management**: Dynamic TTL based on report date and data freshness
- **Cache Invalidation**: Smart cache invalidation based on business rules

**Cache Keys:**
- `unpaid_invoices:{tenant_id}:{date}` - Unpaid invoices cache
- `paid_invoices:{tenant_id}:{date}` - Paid invoices cache  
- `early_paid_invoices:{tenant_id}:{date}` - Early paid invoices cache
- `credit_notes:{tenant_id}:{date}` - Credit notes cache
- `overpayments:{tenant_id}:{date}` - Overpayments cache
- `ar_report:{tenant_id}:{report_date}:{periods}:{period_type}` - Complete report cache

**TTL Calculation:**
```python
def calculate_ttl_for_cache(report_date: str, is_future_date: bool = False) -> int:
    # Future dates: 1 hour cache
    # Recent dates (within 7 days): 24 hours cache  
    # Historical dates: 7 days cache
```

**Benefits:**
- Reduces API calls to Xero by up to 90%
- Faster report generation for repeated requests
- Improved system responsiveness
- Cost optimization for Xero API usage

## 3. Xero API Integration & Business Logic

### 3.1 Unpaid Invoices Processing

The system makes strategic API calls to Xero to fetch invoice data efficiently:

#### 3.1.1 Unpaid Invoices
**Purpose**: Fetch invoices that are currently outstanding
**API Call**: `get_invoices()` with specific filters
**Business Logic**:
```python
# Future report date: Only currently unpaid invoices
where_clause_unpaid = f'Type == "ACCREC" && Status == "AUTHORISED" && AmountDue > 0 && Date <= DateTime({date_for_xero})'

# Past report date: Include invoices outstanding as of report date
where_clause_unpaid = f'Type == "ACCREC" && Date <= DateTime({date_for_xero})'
```

**Why Check Future Dates?**
- **Future Reports**: Only show invoices that are currently unpaid (real-time view)
- **Past Reports**: Show invoices that were outstanding as of that historical date
- **Business Requirement**: Different logic needed for forecasting vs. historical analysis

#### 3.1.2 Paid Invoices (if not future)
**Purpose**: Capture invoices that were paid after their due date
**Business Logic**: `Status == "PAID" && DueDate > DateTime({report_date})`
**Use Case**: Shows invoices that were overdue but eventually paid

#### 3.1.3 Early Paid Invoices (if not future)  
**Purpose**: Handle invoices issued after report date but paid before report date
**Business Logic**: `Date > DateTime({report_date})` with payment verification
**Use Case**: Captures advance payments for future invoices

#### 3.1.4 Additional Scenarios

The system handles 10+ complex business scenarios:

**Scenario 1**: Issue date in June, Payment in June, Due date in July
- **Logic**: Should NOT show in AR (fully paid before due date)
- **Exception**: Show if still has outstanding balance

**Scenario 2**: Issue date in June, Payment in June, Due date in July, no outstanding balance
- **Logic**: Should NOT show in AR

**Scenario 3**: Issue date in June, Not Paid in June, Due date in July  
- **Logic**: Should show in Current bucket

**Scenario 4**: Issue date in July, Paid in June, Due date in July
- **Logic**: Show in Current as NEGATIVE (advance payment)

**Scenario 5**: Issue date before report date, paid before report date
- **Logic**: Should NOT show in AR

**Scenario 6**: Issue date before report date, paid after report date
- **Logic**: Show in Current as POSITIVE

**Scenario 7**: Issue date before report date, due date before report date, partial payments
- **Logic**: Show as NEGATIVE (partial payment)

**Scenario 8**: Outstanding as of report date (unpaid invoices)
- **Logic**: Show in appropriate aging bucket

**Scenario 9**: Issue date before report date, paid on report date
- **Logic**: Should NOT show in AR

**Scenario 10**: Issue date before report date, paid on or before report date
- **Logic**: Should NOT show in AR

#### 3.1.5 Payment Date Processing

**Payment Date Priority System**:
1. **FullyPaidOnDate**: Primary payment date from Xero
2. **Payments Array**: Latest payment date from payments collection
3. **Updated Date**: Use updated_date_utc as proxy for payment
4. **Fallback Logic**: Use issue date for old invoices without payment data

**Date Format Handling**:
```python
# Handle Xero date format: "/Date(1706572800000+0000)/"
if payment_date and isinstance(payment_date, str) and payment_date.startswith('/Date('):
    timestamp_str = payment_date.split('(')[1].split('+')[0]
    timestamp = int(timestamp_str) / 1000  # Convert to seconds
    payment_date = datetime.fromtimestamp(timestamp).date()
```

### 3.2 Credit Notes

**Purpose**: Handle credit notes and their allocations
**API Call**: `get_credit_notes()` with filters
**Business Logic**:
- Include credit notes processed after report date
- Calculate future allocation amounts
- Apply as negative values in aging buckets

**Processing Logic**:
```python
# Check if credit note was processed after report date
if paid_date > report_date:
    should_include = True
    # Calculate future allocation amounts
    for allocation in credit_note.allocations:
        if allocation_date > report_date:
            future_amount += allocation.amount
```

### 3.3 Overpayments

**Purpose**: Handle customer overpayments and advance payments
**API Call**: `get_overpayments()` with filters
**Business Logic**:
- Include overpayments with remaining credit > 0
- Apply as negative values (credits) in aging buckets
- Handle advance payments for future invoices

**Processing Logic**:
```python
# Filter overpayments with remaining credit > 0
filtered_overpayments = [
    op for op in all_overpayments 
    if hasattr(op, 'remaining_credit') and getattr(op, 'remaining_credit', 0) > 0
]
```

## 4. Amount Calculations & System Comments

### 4.1 Amount Calculation Logic

**Core Calculation Engine**:
```python
def process_financial_item(item, report_date, periods, period_of, period_type, bucket_names, report, 
                         amount_field, date_field, is_negative=False, date_fallback=None, 
                         connection_name=None, business_type=None, item_type="invoice", show_current=True):
```

**Key Features**:
- **Dynamic Aging Buckets**: Configurable periods (1-12 months)
- **Flexible Period Types**: Days, Weeks, or Months
- **Multi-currency Support**: Handles different currencies and amounts
- **Negative Amount Handling**: Properly processes credits and overpayments

**Aging Bucket Calculation**:
```python
def calculate_aging_bucket(report_date, due_date, periods: int, period_of: int, period_type: str, show_current: bool = True) -> str:
    days = (report_date - due_date).days
    
    if days < 0:
        return "Current"  # Future due dates
    
    # Month-based calculation using actual calendar months
    if period_type.lower() == "month":
        months_diff = (report_date.year - due_date.year) * 12 + (report_date.month - due_date.month)
        if report_date.day < due_date.day:
            months_diff -= 1
        
        if months_diff <= 0:
            return f"< 1 {period_type}"
        elif months_diff == 1:
            return f"1 {period_type}"
        # ... additional logic for multiple periods
```

### 4.2 System Comments Generation

**Purpose**: Provide detailed breakdown of amounts in each aging bucket
**Function**: `generate_system_comments()`

**Comment Format**:
```
Current:
INV-001 (Invoice, ID: 12345) = 1,500.00
CN-002 (Credit Note, ID: 67890) = -200.00

< 1 Month:
INV-003 (Invoice, ID: 11111) = 2,000.00
OP-001 (Overpayment, ID: 22222) = -500.00
```

**Features**:
- **Item Type Identification**: Distinguishes between invoices, credit notes, overpayments
- **Amount Formatting**: Proper currency formatting with commas
- **Negative Amount Handling**: Shows credits and overpayments as negative values
- **Bucket Organization**: Groups items by aging bucket for clarity

### 4.3 Excel Export Features

**Advanced Excel Generation**:
- **Professional Formatting**: Headers, colors, borders, and styling
- **Dynamic Column Widths**: Auto-adjusted based on content
- **Currency Formatting**: Proper $ formatting for all monetary values
- **Totals & Percentages**: Automatic calculation of totals and percentage distributions
- **Filtering & Sorting**: Built-in Excel filters and sorting capabilities
- **Multi-sheet Support**: Organized data across multiple sheets

**Export Configuration**:
```python
columns = [
    {"header": "Business Unit", "key": "Business Unit", "width": 20, "format": "text"},
    {"header": "Company", "key": "Company", "width": 25, "format": "text"},
    {"header": "Contact", "key": "Contact", "width": 30, "format": "text"},
    {"header": "Current", "key": "Current", "width": 15, "format": "currency"},
    {"header": "< 1 Month", "key": "< 1 Month", "width": 15, "format": "currency"},
    # ... additional aging buckets
    {"header": "Total", "key": "Total", "width": 15, "format": "currency"},
    {"header": "System Comments", "key": "System Comments", "width": 60, "format": "text"}
]
```

## Technical Implementation Details

### Multi-App Support
- **App ID Filtering**: Supports multiple Xero applications (App 1, App 2)
- **Connection Management**: Handles 76+ company connections
- **Token Management**: Automatic token refresh and validation

### Performance Optimizations
- **Pagination**: Efficient handling of large datasets (1000 records per page)
- **Parallel Processing**: Concurrent processing of multiple companies
- **Memory Management**: Optimized data structures and cleanup
- **Database Session Management**: Proper session handling to prevent connection leaks

### Error Handling
- **Graceful Degradation**: Continues processing even if some companies fail
- **Detailed Error Reporting**: Comprehensive error tracking and reporting
- **Retry Mechanisms**: Automatic retry for transient failures
- **Connection Validation**: Pre-processing validation of Xero connections

### Security Features
- **Token Encryption**: Secure storage of Xero access tokens
- **Connection Validation**: Regular validation of Xero connections
- **Error Sanitization**: Safe error message handling
- **Access Control**: Proper authentication and authorization

## Usage Examples

### Generate Report for All Companies
```bash
GET /aged-receivables?report_date=2024-01-31&periods=4&period_type=Month&is_local=true
```

### Generate Report for Specific App
```bash
GET /aged-receivables?app_id=1&report_date=2024-01-31&periods=4&period_type=Month
```

### Queue Report for Background Processing
```bash
GET /aged-receivables?report_date=2024-01-31&periods=4&period_type=Month&is_local=false
```

### Generate Excel Report
```bash
GET /aged-receivables?report_date=2024-01-31&periods=4&period_type=Month&is_response_only=0&format=1
```

## System Requirements

- **Python 3.8+**
- **Redis Server** (for caching)
- **PostgreSQL** (for job queue and connection management)
- **Xero API Access** (with proper OAuth2 credentials)
- **FastAPI** (web framework)
- **OpenPyXL** (Excel generation)

## Configuration

### Environment Variables
- `OUTPUT_DIR`: Directory for Excel file generation
- `REDIS_URL`: Redis connection string
- `DATABASE_URL`: PostgreSQL connection string
- `XERO_CLIENT_ID`: Xero application client ID
- `XERO_CLIENT_SECRET`: Xero application client secret

### Business Configuration
- **Aging Periods**: Configurable 1-12 periods
- **Period Types**: Days, Weeks, or Months
- **Company Filtering**: By App ID or specific connection IDs
- **Cache Settings**: TTL and invalidation rules
- **Export Formats**: JSON, Table, or Excel

This system provides a robust, scalable solution for consolidating aged receivables reports across multiple Xero companies with advanced caching, queuing, and reporting capabilities.
