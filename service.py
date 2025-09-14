from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from fastapi import HTTPException

from xero_python.accounting.api.accounting_api import empty

from app.services.xero_auth import XeroAuthService
from app.util.xero_connection import create_xero_api_client
from app.util.token_manager import TokenManager
from app.database.models import XeroConnection
import os
from app.services.redis_service import RedisService

from app.util.report_helper import generate_bucket_names, process_financial_item, calculate_ttl_for_cache
from app.util.report_export import export_report_to_excel, generate_system_comments
import pandas as pd


class XeroAgedReceivablesService:
    """Service for handling Xero aged receivables report data fetching with multi-app support"""
    
    def __init__(self, xero_auth_service: XeroAuthService):
        self.xero_auth_service = xero_auth_service
        self.token_manager = TokenManager(xero_auth_service)
        # Add session manager for proper database session handling
        from app.util.db_session_manager import DatabaseSessionManager
        self.session_manager = DatabaseSessionManager()
    
    def _safe_float(self, value, default=0.0):
        """Safely convert any numeric value to float"""
        if value is None:
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    def _get_connection_by_id(self, session, connection_id: int):
        """Get connection by ID using session manager"""
        return session.query(XeroConnection).filter(
            XeroConnection.id == connection_id,
            XeroConnection.is_active == True
        ).first()

    def _get_connection_by_tenant_id(self, session, tenant_id: str):
        """Get connection by tenant ID using session manager"""
        return session.query(XeroConnection).filter(
            XeroConnection.tenant_id == tenant_id,
            XeroConnection.is_active == True
        ).first()

    def _get_connections_by_app(self, session, app_id: int):
        """Get connections by app ID using session manager"""
        return session.query(XeroConnection).filter(
            XeroConnection.app_id == app_id,
            XeroConnection.is_active == True
        ).all()

    def _get_all_connections(self, session):
        """Get all active connections using session manager"""
        return session.query(XeroConnection).filter(
            XeroConnection.is_active == True
        ).all()

    def _extract_connection_data(self, connection):
        """Extract connection data into dictionary"""
        return {
            'id': connection.id,
            'tenant_id': connection.tenant_id,
            'tenant_name': connection.tenant_name,
            'app_id': connection.app_id,
            'business_type': getattr(connection, 'business_type', 'Commercial Property'),
            'access_token': connection.access_token,
            'refresh_token': connection.refresh_token,
            'expires_at': connection.expires_at,
            'scope': connection.scope
        }
    

    
    async def get_aged_receivables_data(
        self, 
        connection_data: Dict[str, Any],  # Changed from tenant_id to connection_data
        report_date,
        periods: int = 4,
        period_of: int = 1,
        period_type: str = "Month",
        app_id: Optional[int] = None,
        is_future_date: bool = False,
        redis_service: RedisService = None,
        actual_report_date: str = None
    ) -> Dict[str, Any]:
        """
        Fetch all data needed for aged receivables report
        
        Args:
            connection_data: Dictionary containing connection information (no DB objects)
            report_date: Report date for calculations
            periods: Number of aging periods
            period_of: Duration of each period
            period_type: Type of period (Day, Week, Month)
            app_id: Optional Xero app ID (1-2) for multi-app support
            
        Returns:
            Dict containing invoices, credit_notes, and overpayments
        """
        # Extract tenant_id from connection_data
        tenant_id = str(connection_data['tenant_id'])
        
        # Create a connection object from connection_data for API calls
        # This avoids making database calls during the long calculation
        from app.models.xero_auth import XeroConnection
        
        # Create a temporary connection object with the data we need
        connection = XeroConnection(
            tenant_id=connection_data['tenant_id'],
            tenant_name=connection_data['tenant_name'],
            app_id=connection_data['app_id'],
            access_token=connection_data['access_token'],
            refresh_token=connection_data['refresh_token'],
            expires_at=connection_data['expires_at'],
            scope=connection_data['scope']
        )

        # Ensure we have a valid token before making API calls
        connection = await self.token_manager.ensure_valid_token(connection, tenant_id, app_id)

        # Create Xero API client with manual token management
        accounting_api = create_xero_api_client(connection, tenant_id, self.xero_auth_service)

        cache_ttl = calculate_ttl_for_cache(actual_report_date, True)

        try:
            # Fetch all data needed for the report
            date_for_xero = f"{report_date.year},{report_date.month},{report_date.day}"
            
            # Get invoices
            invoices = self._get_unpaid_invoices(accounting_api, tenant_id, date_for_xero, is_future_date, redis_service, cache_ttl)
            print("--------------------------------")
            print("[DEV DEBUG] Got the invoices", connection_data['tenant_name'])
            print("--------------------------------")
            # invoices = []
            
            # Get credit notes
            credit_notes = self._get_credit_notes(accounting_api, tenant_id, date_for_xero, redis_service, cache_ttl)
            print("--------------------------------")
            print("[DEV DEBUG] Got the credit notes", connection_data['tenant_name'])
            print("--------------------------------")
            # credit_notes = []
            
            # Get bank transactions
            # bank_transactions = self._get_bank_transactions(accounting_api, tenant_id, date_for_xero)

            # Get Overpayments
            overpayments = self._get_overpayments(accounting_api, tenant_id, date_for_xero, redis_service, cache_ttl)
            print("--------------------------------")
            print("[DEV DEBUG] Got the overpayments", connection_data['tenant_name'])
            print("--------------------------------")
            # overpayments = []

            filter_invoices = []
            for invoice in invoices:
                filter_invoices.append({
                    "invoice_number": getattr(invoice, 'invoice_number', None),
                    "invoice_id": getattr(invoice, 'invoice_id', None),
                    "amount_due": float(getattr(invoice, 'amount_due', 0)),
                    "due_date": getattr(invoice, 'due_date', None),
                    "date": getattr(invoice, 'date', None),
                    "status": getattr(invoice, 'status', None),
                    "contact": getattr(invoice.contact, 'name', 'Unknown Contact') if hasattr(invoice, 'contact') and invoice.contact else 'Unknown Contact',
                    "allocations": getattr(invoice, 'allocations', []),
                    "is_negative": getattr(invoice, 'is_negative', False)
                })

            filter_credit_notes = []
            for credit_note in credit_notes:
                filter_credit_notes.append({
                    "credit_note_number": getattr(credit_note, 'credit_note_number', None),
                    "credit_note_id": getattr(credit_note, 'credit_note_id', None),
                    "remaining_credit": float(getattr(credit_note, 'remaining_credit', 0)),
                    "date": getattr(credit_note, 'date', None),
                    "status": getattr(credit_note, 'status', None),
                    "contact": getattr(credit_note.contact, 'name', 'Unknown Contact') if hasattr(credit_note, 'contact') and credit_note.contact else 'Unknown Contact',
                })

            filter_overpayments = []
            for overpayment in overpayments:
                filter_overpayments.append({
                    "overpayment_id": getattr(overpayment, 'overpayment_id', None),
                    "remaining_credit": float(getattr(overpayment, 'remaining_credit', 0)),
                    "date": getattr(overpayment, 'date', None),
                    "status": getattr(overpayment, 'status', None),
                    "contact": getattr(overpayment.contact, 'name', 'Unknown Contact') if hasattr(overpayment, 'contact') and overpayment.contact else 'Unknown Contact',
                })

            return {
                "invoices": filter_invoices,
                "credit_notes": filter_credit_notes,
                "overpayments": filter_overpayments,
                "report_date": report_date,
                "periods": periods,
                "period_of": period_of,
                "period_type": period_type,
                "app_id": connection.app_id
            }
            
        except Exception as e:
            error_message = str(e)
            
            # Check if this is an invalid_grant error
            if "invalid_grant" in error_message.lower():
                # Handle invalid grant error
                # error_details = self.xero_auth_service.handle_invalid_grant_error(
                #     tenant_id, 
                #     connection.app_id, 
                #     error_message
                # )
                raise HTTPException(
                    status_code=401, 
                    detail={
                        "error": "invalid_grant",
                        "message": "Authentication token has expired and needs to be refreshed",
                        "requires_re_authentication": True,
                        "tenant_id": tenant_id,
                        "app_id": connection.app_id
                    }
                )
            
            raise HTTPException(status_code=500, detail=f"Failed to fetch report data: {error_message}")
    
    def _get_unpaid_invoices(self, accounting_api, tenant_id: str, date_for_xero: str, is_future_date: bool, redis_service: RedisService = None, cache_ttl: int = None) -> List:
        """Fetch unpaid and paid invoices with optimized separate calls"""
        
        # Convert date string to datetime for comparison
        from datetime import datetime
        try:
            year, month, day = map(int, date_for_xero.split(','))
            report_date = datetime(year, month, day).date()
        except Exception as e:
            report_date = datetime.now().date()
        
        all_invoices = []
        
        # Call 1: Get AUTHORISED invoices with AmountDue > 0
        unpaid_invoices = []
        page = 1
        page_size = 1000
        order = 'Date DESC'

        print("--------------------------------")
        print("[DEV DEBUG] Getting the unpaid invoices", tenant_id)
        print("--------------------------------")

        # Generate cache key
        cache_key = f"unpaid_invoices:{tenant_id}:{date_for_xero}"
        cached_invoices = redis_service.get_cache(cache_key)
        if cached_invoices:
            print(f"[REDIS] Cache hit for unpaid invoices: {cache_key}")
            unpaid_invoices = cached_invoices
        else:
            # Write a logic to check if report date is future date or past date (today's date considered as past date)
            if is_future_date:
                # Future report date logic: Only include currently unpaid invoices
                where_clause_unpaid = f'Type == "ACCREC" && Status == "AUTHORISED" && AmountDue > 0 && Date <= DateTime({date_for_xero})'
            else:
                # Past report date logic: Include invoices that were outstanding as of the report date
                where_clause_unpaid = f'Type == "ACCREC" && Date <= DateTime({date_for_xero})'
            while True:
                try:
                    try:
                        print("[DEV DEBUG] Unpaid invoices", page)
                        invoices_response = accounting_api.get_invoices(
                            tenant_id,  # xero_tenant_id
                            empty,      # if_modified_since
                            where_clause_unpaid,  # where
                            order,      # order
                            empty,      # ids
                            empty,      # invoice_numbers
                            empty,      # contact_ids
                            ["AUTHORISED", "PAID"] if not is_future_date else ["AUTHORISED"],  # statuses - more efficient than WHERE clause
                            page,       # page
                            empty,      # include_archived
                            empty,      # created_by_my_app
                            empty,      # unitdp
                            "False",    # summary_only - Changed from "True" to "False" to get full details
                            page_size,  # page_size
                            empty       # search_term
                        )
                        
                    except Exception as e:
                        print(f"[DEV DEBUG] ERROR on page {page}: {str(e)}")
                        import traceback
                        print(f"[DEV DEBUG] Full traceback for page {page}: {traceback.format_exc()}")
                        # Continue to next page instead of breaking
                        page += 1
                        continue
                    
                    try:
                        if not invoices_response.invoices:
                            break
                        
                        unpaid_invoices.extend(invoices_response.invoices)
                        
                        # If we got fewer results than page_size, we've reached the end
                        if len(invoices_response.invoices) < page_size:
                            break
                        page += 1
                        
                    except Exception as e:
                        print(f"[DEV DEBUG] ERROR processing response for page {page}: {str(e)}")
                        import traceback
                        print(f"[DEV DEBUG] Full traceback for response processing on page {page}: {traceback.format_exc()}")
                        # Continue to next page instead of breaking
                        page += 1
                        continue
                        
                except Exception as e:
                    print("[DEV DEBUG] Error getting the unpaid invoices", e)
                    break
            
            # Cache the unpaid invoices
            redis_service.set_cache(cache_key, unpaid_invoices, ttl=cache_ttl)

        # Call 2: Get PAID invoices with DueDate > report_date
        paid_invoices = []
        page = 1

        print("--------------------------------")
        print("[DEV DEBUG] Getting the paid invoices", tenant_id)
        print("--------------------------------")
        
        if not is_future_date:
            # Generate cache key
            cache_key = f"paid_invoices:{tenant_id}:{date_for_xero}"
            cached_invoices = redis_service.get_cache(cache_key)
            if cached_invoices:
                print(f"[REDIS] Cache hit for paid invoices: {cache_key}")
                paid_invoices = cached_invoices
            else:
                while True:
                    try:
                        where_clause_paid = f'Type == "ACCREC" && Status == "PAID" && DueDate > DateTime({date_for_xero})'
                        invoices_response = accounting_api.get_invoices(
                            tenant_id,  # xero_tenant_id
                            empty,      # if_modified_since
                            where_clause_paid,  # where
                            order,      # order
                            empty,      # ids
                            empty,      # invoice_numbers
                            empty,      # contact_ids
                            ["PAID"],   # statuses
                            page,       # page
                            empty,      # include_archived
                            empty,      # created_by_my_app
                            empty,      # unitdp
                            "False",    # summary_only - Changed from "True" to "False" to get full details
                            page_size,  # page_size
                            empty       # search_term
                        )
                        
                        if not invoices_response.invoices:
                            break
                        
                        paid_invoices.extend(invoices_response.invoices)
                        
                        if len(invoices_response.invoices) < page_size:
                            break
                        
                        if page >= 100:  # Safety limit
                            break
                            
                        page += 1
                        
                    except Exception as e:
                        print("[DEV DEBUG] Error getting the paid invoices", e)
                        break 
                # Cache the paid invoices
                redis_service.set_cache(cache_key, paid_invoices, ttl=cache_ttl)
        
        # Call 3: Get invoices issued after report date but paid before report date (for past reports)
        # This handles the case where an invoice was issued in July but paid in June
        early_paid_invoices = []
        page = 1

        print("--------------------------------")
        print("[DEV DEBUG] Getting the early paid invoices", tenant_id)
        print("--------------------------------")
        
        if not is_future_date:
            # Generate cache key
            cache_key = f"early_paid_invoices:{tenant_id}:{date_for_xero}"
            cached_invoices = redis_service.get_cache(cache_key)
            if cached_invoices:
                print(f"[REDIS] Cache hit for early paid invoices: {cache_key}")
                early_paid_invoices = cached_invoices
            else:
                while True:
                    try:
                        where_clause_early_paid = f'Type == "ACCREC" && Date > DateTime({date_for_xero})'
                        invoices_response = accounting_api.get_invoices(
                            tenant_id,  # xero_tenant_id
                            empty,      # if_modified_since
                            where_clause_early_paid,  # where
                            order,      # order
                            empty,      # ids
                            empty,      # invoice_numbers
                            empty,      # contact_ids
                            ["PAID", "AUTHORISED"],   # statuses - include both to catch all cases
                            page,       # page
                            empty,      # include_archived
                            empty,      # created_by_my_app
                            empty,      # unitdp
                            "False",    # summary_only - Changed from "True" to "False" to get full details
                            page_size,  # page_size
                            empty       # search_term
                        )
                        
                        if not invoices_response.invoices:
                            break
                        
                        early_paid_invoices.extend(invoices_response.invoices)
                        
                        if len(invoices_response.invoices) < page_size:
                            break
                        
                        if page >= 100:  # Safety limit
                            break
                            
                        page += 1
                        
                    except Exception as e:
                        print("[DEV DEBUG] Error getting the early invoices", e)
                        break
                # Cache the early paid invoices
                redis_service.set_cache(cache_key, early_paid_invoices, ttl=cache_ttl)
            
        print("--------------------------------")
        print("[DEV DEBUG] Looping through unpaid invoices", tenant_id)
        print("--------------------------------")
        
        for invoice in unpaid_invoices:
            if (is_future_date and invoice.type == "ACCREC" and 
                invoice.amount_due > 0 and 
                invoice.status == "AUTHORISED" and
                invoice.date and invoice.date <= report_date):
                all_invoices.append(invoice)

            elif (not is_future_date and invoice.type == "ACCREC" and 
                invoice.date and invoice.date <= report_date):
                # Past report logic: Implement specific scenarios
                issue_date = getattr(invoice, 'date', None)
                due_date = getattr(invoice, 'due_date', None)
                payment_date = getattr(invoice, 'fully_paid_on_date', None)
                status = getattr(invoice, 'status', None)
                amount_due = getattr(invoice, 'amount_due', 0)
                total_amount = getattr(invoice, 'total', 0)
                
                # Get payment date from Payments array if available
                payments = getattr(invoice, 'payments', [])
                amount_paid = getattr(invoice, 'amount_paid', 0)
                
                # First priority: Check FullyPaidOnDate
                payment_date = getattr(invoice, 'fully_paid_on_date', None)
                
                # Parse FullyPaidOnDate if it's in Xero date format
                if payment_date and isinstance(payment_date, str) and payment_date.startswith('/Date('):
                    try:
                        # Extract timestamp from "/Date(1706572800000+0000)/"
                        timestamp_str = payment_date.split('(')[1].split('+')[0]
                        timestamp = int(timestamp_str) / 1000  # Convert to seconds
                        payment_date = datetime.fromtimestamp(timestamp).date()
                    except (ValueError, IndexError):
                        payment_date = None
                elif payment_date and isinstance(payment_date, str) and payment_date.startswith('\\/Date('):
                    try:
                        # Extract timestamp from "\/Date(1706572800000+0000)\/"
                        timestamp_str = payment_date.split('(')[1].split('+')[0]
                        timestamp = int(timestamp_str) / 1000  # Convert to seconds
                        payment_date = datetime.fromtimestamp(timestamp).date()
                    except (ValueError, IndexError):
                        payment_date = None
                
                # Second priority: Get payment date from Payments array if FullyPaidOnDate not available
                if not payment_date and payments:
                    # Get the latest payment date
                    latest_payment_date = None
                    for payment in payments:
                        if hasattr(payment, 'date'):
                            payment_dt = payment.date
                            # Handle Xero date format: "/Date(1592179200000+0000)/"
                            if isinstance(payment_dt, str) and payment_dt.startswith('/Date('):
                                try:
                                    # Extract timestamp from "/Date(1592179200000+0000)/"
                                    timestamp_str = payment_dt.split('(')[1].split('+')[0]
                                    timestamp = int(timestamp_str) / 1000  # Convert to seconds
                                    payment_dt = datetime.fromtimestamp(timestamp).date()
                                except (ValueError, IndexError):
                                    continue
                            elif isinstance(payment_dt, str) and payment_dt.startswith('\\/Date('):
                                try:
                                    # Extract timestamp from "\/Date(1592179200000+0000)\/"
                                    timestamp_str = payment_dt.split('(')[1].split('+')[0]
                                    timestamp = int(timestamp_str) / 1000  # Convert to seconds
                                    payment_dt = datetime.fromtimestamp(timestamp).date()
                                except (ValueError, IndexError):
                                    continue
                            elif hasattr(payment_dt, 'date'):
                                payment_dt = payment_dt.date()
                            else:
                                continue
                            
                            if latest_payment_date is None or payment_dt > latest_payment_date:
                                latest_payment_date = payment_dt
                    
                    if latest_payment_date:
                        payment_date = latest_payment_date
                
                # Third priority: If no payments found but amount_paid > 0, use updated_date_utc as proxy
                if amount_paid > 0 and not payment_date:
                    updated_date = getattr(invoice, 'updated_date_utc', None)
                    if updated_date and hasattr(updated_date, 'date'):
                        payment_date = updated_date.date()
                
                # Fourth priority: If still no payment date but invoice is clearly paid (amount_paid > 0 and amount_due = 0)
                # and issue_date is before report_date, assume it was paid before report_date
                if amount_paid > 0 and amount_due == 0 and not payment_date and issue_date and issue_date <= report_date:
                    # Create a dummy payment date that's before the report date
                    payment_date = issue_date  # Use issue date as payment date for old invoices
                
                # Convert dates to date objects if needed
                if issue_date and hasattr(issue_date, 'date'):
                    issue_date = issue_date.date()
                if due_date and hasattr(due_date, 'date'):
                    due_date = due_date.date()
                if payment_date and hasattr(payment_date, 'date'):
                    payment_date = payment_date.date()

                adjusted_total_amount = float(total_amount)
                adjusted_amount_due = float(amount_due)
                
                should_include = False
                is_negative = False
                report_amount = 0

                # Scenario 1: Issue date in June, Payment in June, Due date in July - SHOULD NOT SHOW IN AR
                if (issue_date and issue_date <= report_date and 
                    payment_date and payment_date <= report_date and 
                    due_date and due_date > report_date):
                    # Scenario 1a: Issue date in June, Payment in June, Due date in July, but still has outstanding balance - SHOULD SHOW IN AR
                    if (issue_date and issue_date <= report_date and 
                      payment_date and payment_date <= report_date and
                      amount_due > 0):  # Include if there's still money owed
                        should_include = True
                        is_negative = False
                        report_amount = adjusted_amount_due
                    else:
                        should_include = False

                # Scenario 2: Issue date in June, Payment in June, Due date in July, but no outstanding balance - SHOULD NOT SHOW IN AR
                elif (issue_date and issue_date <= report_date and 
                      payment_date is not None and payment_date <= report_date and 
                      due_date and due_date <= report_date and
                      amount_due == 0):  # Only for fully paid invoices
                    should_include = False
                
                # Scenario 3: Issue date in June, Not Paid in June, Due date in July - SHOULD SHOW IN CURRENT
                elif (issue_date and issue_date <= report_date and 
                      (not payment_date or payment_date > report_date) and 
                      due_date and due_date > report_date):
                    should_include = True
                    is_negative = False
                    credit_note_after_report = False
                    if hasattr(invoice, 'credit_notes') and invoice.credit_notes:
                        for credit_note in invoice.credit_notes:
                            if hasattr(credit_note, 'date') and credit_note.date:
                                credit_note_date = credit_note.date
                                if hasattr(credit_note_date, 'date'):
                                    credit_note_date = credit_note_date.date()
                                
                                # If credit note date is after report date, don't include this invoice
                                if credit_note_date > report_date:
                                    credit_note_after_report = True
                                    break
                    
                    if credit_note_after_report:
                        report_amount = adjusted_total_amount
                    else:
                        report_amount = adjusted_amount_due if adjusted_amount_due > 0 else adjusted_total_amount

                # Scenario 4: Issue date in July, Paid in June, Due date in July - SHOULD SHOW IN CURRENT AS NEGATIVE
                elif (issue_date and issue_date > report_date and 
                      payment_date and payment_date <= report_date and 
                      due_date and due_date >= report_date):
                    should_include = True
                    is_negative = True
                    report_amount = adjusted_total_amount

                # Scenario 5: Issue date before report date, paid before report date - SHOULD NOT SHOW IN AR
                elif (issue_date and issue_date <= report_date and 
                      payment_date and payment_date <= report_date and
                      amount_due == 0):  # Only for fully paid invoices
                    should_include = False
                
                # Scenario 6: Issue date before report date, paid after report date - SHOULD SHOW IN CURRENT AS POSITIVE
                elif (issue_date and issue_date <= report_date and 
                      payment_date and payment_date > report_date):
                    should_include = True
                    is_negative = False  # Changed to False - normal payment should be positive                                       
                    # Calculate payments made after report date
                    payments_after_report = 0.0
                    payments = getattr(invoice, 'payments', [])
                    
                    if payments:
                        for payment in payments:
                            if hasattr(payment, 'date'):
                                payment_dt = payment.date
                                payment_amount = getattr(payment, 'amount', 0.0)
                                
                                # Handle Xero date format
                                if isinstance(payment_dt, str) and payment_dt.startswith('/Date('):
                                    try:
                                        timestamp_str = payment_dt.split('(')[1].split('+')[0]
                                        timestamp = int(timestamp_str) / 1000
                                        payment_dt = datetime.fromtimestamp(timestamp).date()
                                    except (ValueError, IndexError):
                                        continue
                                elif isinstance(payment_dt, str) and payment_dt.startswith('\\/Date('):
                                    try:
                                        timestamp_str = payment_dt.split('(')[1].split('+')[0]
                                        timestamp = int(timestamp_str) / 1000
                                        payment_dt = datetime.fromtimestamp(timestamp).date()
                                    except (ValueError, IndexError):
                                        continue
                                elif hasattr(payment_dt, 'date'):
                                    payment_dt = payment_dt.date()
                                elif hasattr(payment_dt, 'year') and hasattr(payment_dt, 'month') and hasattr(payment_dt, 'day'):
                                    # Already a date object
                                    pass
                                else:
                                    continue
                                
                                # Only include payments made after report date
                                if payment_dt > report_date:
                                    payments_after_report += float(payment_amount)

                        payments_after_report += float(amount_due)
                    
                    # Use payments after report date if available, otherwise use adjusted_total_amount
                    if payments_after_report > 0:
                        report_amount = payments_after_report
                    elif amount_due == 0:
                        report_amount = adjusted_total_amount
                    else:
                        report_amount = adjusted_amount_due
                
                # Scenario 7: Issue date before report date, due date before report date, partial payments - SHOULD SHOW AS NEGATIVE
                elif (issue_date and issue_date <= report_date and 
                      due_date and due_date <= report_date and
                      amount_paid > 0 and amount_due > 0):
                    should_include = True
                    is_negative = False
                    report_amount = adjusted_amount_due  # Show the partial payment amount as negative

                # Scenario 8: Include if it was outstanding as of report date (unpaid invoices)
                elif (issue_date and issue_date <= report_date and 
                      (not payment_date or payment_date > report_date)):
                    should_include = True
                    is_negative = False
                    report_amount = adjusted_amount_due if adjusted_amount_due > 0 else adjusted_total_amount  

                # Scenario 9: Issue date before report date, paid on report date - SHOULD NOT SHOW IN AR
                elif (issue_date and issue_date <= report_date and 
                      payment_date and payment_date == report_date):
                    should_include = False

                # Scenario 10: Issue date before report date, paid on or before report date - SHOULD NOT SHOW IN AR
                elif (issue_date and issue_date <= report_date and 
                      payment_date and payment_date <= report_date):
                    should_include = False
                if should_include:
                    # Create a modified invoice object with the correct amount
                    modified_invoice = type("Item", (), {})()
                    for attr in dir(invoice):
                        if not attr.startswith('_'):
                            setattr(modified_invoice, attr, getattr(invoice, attr))
                    
                    # Override the amount_due with our calculated amount
                    setattr(modified_invoice, 'amount_due', report_amount)
                    setattr(modified_invoice, 'is_negative', is_negative)
                    
                    all_invoices.append(modified_invoice)
                else:
                    pass
        
        # Filter PAID invoices with business logic:
        # - DueDate > report_date
        # - Issue date and Due date must be in the same month (to avoid showing invoices issued in one month but due in another)
        print("--------------------------------")
        print("[DEV DEBUG] Looping through paid invoices", tenant_id)
        print("--------------------------------")
        for invoice in paid_invoices:
            if (invoice.type == "ACCREC" and
                invoice.status == "PAID" and
                invoice.due_date and invoice.due_date > report_date and
                invoice.date and invoice.due_date):
                
                # Check if issue date and due date are in the same month
                issue_date = invoice.date
                due_date = invoice.due_date
                
                # Convert to date objects if they're datetime objects
                if hasattr(issue_date, 'date'):
                    issue_date = issue_date.date()
                if hasattr(due_date, 'date'):
                    due_date = due_date.date()
                
                # Check if both dates are in the same month and year
                if (issue_date.year == due_date.year and 
                    issue_date.month == due_date.month and
                    issue_date <= report_date):
                    all_invoices.append(invoice)

        # Filter EARLY PAID invoices (issued after report date but paid before report date)
        # This handles the specific case where an invoice was issued in July but paid in June
        print("--------------------------------")
        print("[DEV DEBUG] Looping through early paid invoices", tenant_id)
        print("--------------------------------")
        for invoice in early_paid_invoices:
            if invoice.type == "ACCREC":
                # Get issue date first for the initial check
                issue_date = getattr(invoice, 'date', None)
                if issue_date and hasattr(issue_date, 'date'):
                    issue_date = issue_date.date()
                elif issue_date and isinstance(issue_date, str):
                    try:
                        issue_date = datetime.strptime(issue_date[:10], "%Y-%m-%d").date()
                    except:
                        issue_date = None
                
                # Skip if invoice was not issued after report date
                if not issue_date or issue_date <= report_date:
                    continue
                # Get payment date from the invoice
                payment_date = getattr(invoice, 'fully_paid_on_date', None)
                payments = getattr(invoice, 'payments', [])
                amount_paid = getattr(invoice, 'amount_paid', 0)
                
                # Parse FullyPaidOnDate if it's in Xero date format
                if payment_date and isinstance(payment_date, str) and payment_date.startswith('/Date('):
                    try:
                        timestamp_str = payment_date.split('(')[1].split('+')[0]
                        timestamp = int(timestamp_str) / 1000
                        payment_date = datetime.fromtimestamp(timestamp).date()
                    except (ValueError, IndexError):
                        payment_date = None
                elif payment_date and isinstance(payment_date, str) and payment_date.startswith('\\/Date('):
                    try:
                        timestamp_str = payment_date.split('(')[1].split('+')[0]
                        timestamp = int(timestamp_str) / 1000
                        payment_date = datetime.fromtimestamp(timestamp).date()
                    except (ValueError, IndexError):
                        payment_date = None
                
                # Get payment date from Payments array if FullyPaidOnDate not available
                # Also calculate total paid up to report_date for early payments
                if payments:
                    # Calculate total amount paid up to report_date
                    total_paid_up_to_report_date = 0.0
                    has_payments_before_report_date = False
                    
                    for payment in payments:
                        if hasattr(payment, 'date'):
                            payment_dt = payment.date
                            payment_amount = getattr(payment, 'amount', 0.0)
                            
                            if isinstance(payment_dt, str) and payment_dt.startswith('/Date('):
                                try:
                                    timestamp_str = payment_dt.split('(')[1].split('+')[0]
                                    timestamp = int(timestamp_str) / 1000
                                    payment_dt = datetime.fromtimestamp(timestamp).date()
                                except (ValueError, IndexError):
                                    continue
                            elif isinstance(payment_dt, str) and payment_dt.startswith('\\/Date('):
                                try:
                                    timestamp_str = payment_dt.split('(')[1].split('+')[0]
                                    timestamp = int(timestamp_str) / 1000
                                    payment_dt = datetime.fromtimestamp(timestamp).date()
                                except (ValueError, IndexError):
                                    continue
                            elif hasattr(payment_dt, 'date'):
                                payment_dt = payment_dt.date()
                            elif hasattr(payment_dt, 'year') and hasattr(payment_dt, 'month') and hasattr(payment_dt, 'day'):
                                # Already a date object, no conversion needed
                                pass
                            else:
                                continue

                            if payment_dt <= report_date:
                                total_paid_up_to_report_date += float(payment_amount)
                                has_payments_before_report_date = True
                    
                    # If we found payments before or on report_date, set payment_date and store the amount
                    if has_payments_before_report_date:
                        payment_date = report_date
                        setattr(invoice, 'total_paid_up_to_report_date', total_paid_up_to_report_date)
                
                # Convert dates to date objects if needed
                issue_date = getattr(invoice, 'date', None)
                due_date = getattr(invoice, 'due_date', None)
                total_amount = getattr(invoice, 'total', 0)
                
                if issue_date and hasattr(issue_date, 'date'):
                    issue_date = issue_date.date()
                if due_date and hasattr(due_date, 'date'):
                    due_date = due_date.date()
                if payment_date and hasattr(payment_date, 'date'):
                    payment_date = payment_date.date()
                
                # Check if this invoice matches the scenario: issued after report date, paid before report date
                # OR if it's an AUTHORISED invoice with amount_paid > 0 (indicating it was paid)
                condition1 = (issue_date and issue_date > report_date and 
                             payment_date and payment_date <= report_date and 
                             due_date and due_date >= report_date)
                condition2 = (getattr(invoice, 'status', '') == "AUTHORISED" and 
                             amount_paid > 0 and 
                             issue_date and issue_date > report_date and
                             due_date and due_date >= report_date and
                             has_payments_before_report_date)
                
                if condition1 or condition2:  # Only include if payments were made before report date
                    
                    # Additional check: Ensure payment was actually made before report date
                    if payment_date and payment_date > report_date:
                        continue  # Skip if payment was made after report date
                    
                    # Create a modified invoice object for this scenario
                    modified_invoice = type("Item", (), {})()
                    for attr in dir(invoice):
                        if not attr.startswith('_'):
                            setattr(modified_invoice, attr, getattr(invoice, attr))
                    
                    # Set as negative amount (credit) in Current bucket
                    # Use total_paid_up_to_report_date if available, otherwise use total_amount
                    if hasattr(invoice, 'total_paid_up_to_report_date'):
                        report_amount = getattr(invoice, 'total_paid_up_to_report_date', total_amount)
                    else:
                        report_amount = total_amount
                    
                    setattr(modified_invoice, 'amount_due', report_amount)
                    setattr(modified_invoice, 'is_negative', True)
                    
                    all_invoices.append(modified_invoice)

        return all_invoices
    
    def _get_credit_notes(self, accounting_api, tenant_id: str, date_for_xero: str, redis_service: RedisService = None, cache_ttl: int = None) -> List:
        """Fetch all credit notes for the period"""
        # Convert date string to datetime for comparison
        from datetime import datetime
        try:
            year, month, day = map(int, date_for_xero.split(','))
            report_date = datetime(year, month, day).date()
        except Exception as e:
            report_date = datetime.now().date()

        page = 1
        page_size = 1000
        all_credit_notes = []
        
        credit_where_clauses = []
        credit_where_clauses.append(f'Type == "ACCRECCREDIT"')
        credit_where_clauses.append(f"Date <= DateTime({date_for_xero})")
        credit_where_clauses.append(f'(Status == "PAID" OR Status == "AUTHORISED")')
        credit_where_clause = " && ".join(credit_where_clauses)
        
        # Generate cache key
        cache_key = f"credit_notes:{tenant_id}:{date_for_xero}"
        cached_credit_notes = redis_service.get_cache(cache_key)
        if cached_credit_notes:
            print(f"[REDIS] Cache hit for credit notes: {cache_key}")
            all_credit_notes = cached_credit_notes
        else:
            while True:
                try:
                    credit_notes_response = accounting_api.get_credit_notes(
                        tenant_id,
                        empty,  # if_modified_since
                        credit_where_clause,
                        'Date DESC',  # order
                        page,   # page
                        empty,  # unitdp
                        page_size  # page_size
                    )
                
                    if not credit_notes_response.credit_notes:
                        break
                    
                    all_credit_notes.extend(credit_notes_response.credit_notes)
                    
                    if len(credit_notes_response.credit_notes) < page_size:
                        break

                    page += 1

                except Exception as e:
                    break
            
            # Cache the credit notes
            redis_service.set_cache(cache_key, all_credit_notes, ttl=cache_ttl)

        # Filter credit notes based on processing date logic
        filtered_credit_notes = []
        for credit_note in (all_credit_notes or []):
            should_include = False
            
            # Check if credit note was processed after report date
            if hasattr(credit_note, 'fully_paid_on_date') and credit_note.fully_paid_on_date:
                # Convert to date if it's datetime
                paid_date = credit_note.fully_paid_on_date
                if hasattr(paid_date, 'date'):
                    paid_date = paid_date.date()
                
                if paid_date > report_date:
                    should_include = True
                    
                    # If there are allocations, calculate future date values
                    if hasattr(credit_note, 'allocations') and credit_note.allocations:
                        future_amount = 0.0
                        for allocation in credit_note.allocations:
                            if hasattr(allocation, 'date') and hasattr(allocation, 'amount'):
                                allocation_date = allocation.date
                                if hasattr(allocation_date, 'date'):
                                    allocation_date = allocation_date.date()
                                
                                # Only include allocations processed after report date
                                if allocation_date > report_date:
                                    future_amount += self._safe_float(getattr(allocation, 'amount', 0))
                        
                        # Set the future amount as remaining_credit
                        credit_note.remaining_credit = future_amount
                    else:
                        # If no allocations, use total amount
                        credit_note.remaining_credit = self._safe_float(getattr(credit_note, 'total', 0))
            
            # Check payments if no FullyPaidOnDate
            elif hasattr(credit_note, 'payments') and credit_note.payments:
                latest_payment_date = None
                for payment in credit_note.payments:
                    if hasattr(payment, 'date'):
                        payment_date = payment.date
                        if hasattr(payment_date, 'date'):
                            payment_date = payment_date.date()
                        
                        if latest_payment_date is None or payment_date > latest_payment_date:
                            latest_payment_date = payment_date
                
                if latest_payment_date and latest_payment_date > report_date:
                    should_include = True
            
            # Check allocations if no payments
            elif hasattr(credit_note, 'allocations') and credit_note.allocations:
                latest_allocation_date = None
                for allocation in credit_note.allocations:
                    if hasattr(allocation, 'date'):
                        allocation_date = allocation.date
                        if hasattr(allocation_date, 'date'):
                            allocation_date = allocation_date.date()
                        
                        if latest_allocation_date is None or allocation_date > latest_allocation_date:
                            latest_allocation_date = allocation_date
                
                if latest_allocation_date and latest_allocation_date <= report_date:
                    should_include = True
            
            # If no processing date found, include it (for AUTHORISED credit notes)
            else:
                should_include = True
            
            if should_include:
                filtered_credit_notes.append(credit_note)
        
        return filtered_credit_notes
    
    def _get_bank_transactions(self, accounting_api, tenant_id: str, date_for_xero: str) -> List:
        """Fetch bank transactions with type RECEIVE-OVERPAYMENT for the period"""
        bank_where_clauses = ['Type == "RECEIVE-OVERPAYMENT"']
        bank_where_clauses.append(f"Date <= DateTime({date_for_xero})")
        bank_where_clauses.append(f'Status == "AUTHORISED"')
        bank_where_clauses.append(f'IsReconciled == false')
        bank_where_clause = " && ".join(bank_where_clauses)
        
        bank_transactions_response = accounting_api.get_bank_transactions(
            tenant_id,
            empty,  # if_modified_since
            bank_where_clause,
            empty,  # order
            empty,  # ids
            empty,  # bank_account_ids
            empty,  # statuses
        )
        
        return bank_transactions_response.bank_transactions or []

    def _get_overpayments(self, accounting_api, tenant_id: str, date_for_xero: str, redis_service: RedisService = None, cache_ttl: int = None) -> List:
        """Fetch overpayments with remaining credit for the period"""
        all_overpayments = []
        page = 1
        page_size = 1000

        # Generate cache key
        cache_key = f"overpayments:{tenant_id}:{date_for_xero}"
        cached_overpayments = redis_service.get_cache(cache_key)
        if cached_overpayments:
            print(f"[REDIS] Cache hit for overpayments: {cache_key}")
            all_overpayments = cached_overpayments
        else:
            while True:
                overpayment_clause = []
                overpayment_clause = ['Type == "RECEIVE-OVERPAYMENT"']
                overpayment_clause.append(f"Date <= DateTime({date_for_xero})")
                overpayment_clause.append(f'Status == "AUTHORISED"')
                overpayment_clause = " && ".join(overpayment_clause)
                
                overpayment_response = accounting_api.get_overpayments(
                    tenant_id,
                    empty,  # if_modified_since
                    overpayment_clause,
                    'Date DESC',  # order
                    page,   # page
                    empty,  # unitdp
                    page_size  # page_size
                )
                
                if not overpayment_response.overpayments:
                    break
                    
                all_overpayments.extend(overpayment_response.overpayments)
                
                # If we got fewer results than page_size, we've reached the end
                if len(overpayment_response.overpayments) < page_size:
                    break
                    
                page += 1

            # Cache the overpayments
            redis_service.set_cache(cache_key, all_overpayments, ttl=cache_ttl)

        # Filter overpayments with remaining credit > 0
        filtered_overpayments = [
            op for op in all_overpayments 
            if hasattr(op, 'remaining_credit') and getattr(op, 'remaining_credit', 0) > 0
        ]
        
        return filtered_overpayments

    
    @classmethod
    def get_service_dependency(cls):
        """FastAPI dependency function for XeroAgedReceivablesService"""
        from fastapi import Depends
        from app.services.xero_auth import XeroAuthService
        
        def _get_service(xero_auth_service: XeroAuthService = Depends(XeroAuthService.get_service_dependency())) -> XeroAgedReceivablesService:
            return cls(xero_auth_service)
        
        return _get_service

    async def generate_aged_receivables_report(
        self,
        report_date: str = None,
        periods: int = 4,
        period_of: int = 1,
        period_type: str = "Month",
        app_id: Optional[int] = None,
        show_current: bool = True,
        connection_id: str = None,
        is_response_only: int = 1,
        format: int = 1,
        is_cache: bool = False,
        email: str = None
    ) -> Dict[str, Any]:
        """
        Generate complete aged receivables report with Excel export
        
        Args:
            report_date: Report date in YYYY-MM-DD format
            periods: Number of aging periods
            period_of: Duration of each period
            period_type: Type of period (Day, Week, Month)
            app_id: Optional Xero app ID (1-2) for multi-app support
            show_current: Show Current bucket separately
            connection_id: Connection ID(s) - comma-separated for multiple connections
            is_response_only: If 1, return response only without Excel generation
            format: If 1, return table format; if 0, return JSON format
            is_cache: Use cache (true) or not (false)
            email: Email address to send the report to (optional)
        Returns:
            Dict containing report data and Excel file path
        """
        
        # Parse report_date or use today
        if report_date:
            try:
                parsed_date = datetime.strptime(report_date, "%Y-%m-%d").date()
                report_date_obj = parsed_date
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid report_date format. Use YYYY-MM-DD.")
        else:
            report_date_obj = datetime.utcnow().date()

        is_future_date = report_date_obj > datetime.now().date()

        # Get database session for retrieving connections
        session = None
        failed_connections = []  # Track failed connections from initial retrieval
        connections = []
        connection_data_list = []  # Store connection data without DB objects
        
        try:
            # Get fresh session for this operation
            session = self.session_manager.get_fresh_session()
            
            # Get all active connections with app_id filtering
            if connection_id:
                # Handle comma-separated connection IDs
                connection_ids = [cid.strip() for cid in connection_id.split(',')]
                for cid in connection_ids:
                    try:
                        # Try to parse as integer for connection ID
                        connection_id_int = int(cid)
                        connection = self._get_connection_by_id(session, connection_id_int)
                        if connection:
                            connections.append(connection)
                            # Extract connection data (no DB objects)
                            connection_data = self._extract_connection_data(connection)
                            connection_data_list.append(connection_data)
                        else:
                            # Connection not found
                            failed_connections.append({
                                "connection_id": cid,
                                "tenant_id": None,
                                "tenant_name": "Unknown",
                                "app_id": None,
                                "error": "Connection not found",
                                "error_details": f"Connection with ID {cid} was not found in the database"
                            })
                    except ValueError:
                        # If not an integer, try as tenant_id
                        try:
                            connection = self._get_connection_by_tenant_id(session, cid)
                            if connection:
                                connections.append(connection)
                                # Extract connection data (no DB objects)
                                connection_data = self._extract_connection_data(connection)
                                connection_data_list.append(connection_data)
                            else:
                                failed_connections.append({
                                    "connection_id": cid,
                                    "tenant_id": None,
                                    "tenant_name": "Unknown",
                                    "app_id": None,
                                    "error": "Connection not found",
                                    "error_details": f"Connection with ID {cid} was not found in the database"
                                })
                        except Exception as e:
                            failed_connections.append({
                                "connection_id": cid,
                                "tenant_id": None,
                                "tenant_name": "Unknown",
                                "app_id": None,
                                "error": str(e),
                                "error_details": f"Failed to retrieve connection {cid}: {str(e)}"
                            })
                    except Exception as e:
                        # Log error but continue with other connections
                        print(f"Error getting connection {cid}: {str(e)}")
                        failed_connections.append({
                            "connection_id": cid,
                            "tenant_id": None,
                            "tenant_name": "Unknown",
                            "app_id": None,
                            "error": str(e),
                            "error_details": f"Failed to retrieve connection {cid}: {str(e)}"
                        })
            elif app_id:
                connections = self._get_connections_by_app(session, app_id)
                # Extract connection data for all connections
                for connection in connections:
                    connection_data = self._extract_connection_data(connection)
                    connection_data_list.append(connection_data)
            else:
                connections = self._get_all_connections(session)
                # Extract connection data for all connections
                for connection in connections:
                    connection_data = self._extract_connection_data(connection)
                    connection_data_list.append(connection_data)
            
            if not connections and not failed_connections:
                raise HTTPException(status_code=404, detail="No active Xero connections found")
                
        finally:
            # CRITICAL: Close the database session immediately after getting connection data
            if session:
                self.session_manager.close_session(session)
                print("[AGED_RECEIVABLES] Database session closed after retrieving connection data")
        
        # IMPORTANT: At this point, all database connections are closed.
        # We now have connection_data_list with all the data we need.
        # The long calculation (4-5 minutes) will happen WITHOUT any database connections open.
        # This prevents "idle in transaction" issues and connection timeouts.

        print("--------------------------------")
        print("[DEV DEBUG] Got the connection List", connection_data_list)
        print("--------------------------------")

        # Generate bucket names based on configurable periods
        bucket_names = generate_bucket_names(periods, period_type, show_current)
        
        all_report_data = {}
        total_invoices = 0

        start_time = datetime.now()
        print("--------------------------------")
        print("[DEV DEBUG] Start Time", start_time)
        print("--------------------------------")

        cache_ttl = calculate_ttl_for_cache(report_date)
        
        # Process each connection using connection data (no DB objects)
        for connection_data in connection_data_list:
            try:
                redis_service = RedisService()
                # Redis cache key generation
                cache_key = f"ar_report:{connection_data['tenant_id']}:{report_date}:{periods}:{period_type}"
                cached_data = redis_service.get_cache(cache_key)
                if cached_data and is_cache:
                    print(f"[REDIS] Cache hit for key: {cache_key}")
                    data = cached_data
                else:
                    # Fetch data for this connection with app_id support
                    data = await self.get_aged_receivables_data(
                        connection_data=connection_data,  # Pass connection data instead of tenant_id
                        report_date=report_date_obj,
                        periods=periods,
                        period_of=period_of,
                        period_type=period_type,
                        app_id=connection_data['app_id'],
                        is_future_date=is_future_date,
                        redis_service=redis_service,
                        actual_report_date=report_date
                    )

                    redis_service.set_cache(cache_key, data, ttl=cache_ttl)
                    print(f"[REDIS] Cached data for key: {cache_key} with TTL: {cache_ttl}s")
                
                invoices = data["invoices"]
                credit_notes = data["credit_notes"]
                overpayments = data["overpayments"]

                print("--------------------------------")
                print("[DEV DEBUG] Connection Data", connection_data['tenant_name'])
                print("[DEV DEBUG] Invoices", len(invoices))
                print("[DEV DEBUG] Credit Notes", len(credit_notes))
                print("[DEV DEBUG] Overpayments", len(overpayments))
                print("--------------------------------")

                # print(f"[DEBUG] Overpayments: {overpayments}")
                # print(f"[DEBUG] Credit notes: {credit_notes}")
                # print(f"[DEBUG] Invoices: {invoices}")
                
                total_invoices += len(invoices)

                # Process invoices
                # First, group invoices by contact to handle multiple paid invoices per contact
                contact_invoices = {}

                print("--------------------------------")
                print("[DEV DEBUG] Connection Data", connection_data['tenant_name'])
                print("[DEV DEBUG] Looping through invoices to handle multiple paid invoices per contact")
                print("--------------------------------")
                
                for invoice in invoices:
                    contact_name = invoice.get("contact", "Unknown Contact")
                    if contact_name not in contact_invoices:
                        contact_invoices[contact_name] = []
                    contact_invoices[contact_name].append(invoice)
                
                print("--------------------------------")
                print("[DEV DEBUG] Connection Data", connection_data['tenant_name'])
                print("[DEV DEBUG] Looping through contact invoices to handle multiple paid invoices per contact")
                print("--------------------------------")
                # Process each contact's invoices
                for contact_name, contact_invoice_list in contact_invoices.items():
                    # Create unique key for this contact
                    business_type = connection_data['business_type']
                    key = f"{business_type}|{connection_data['tenant_name']}|{contact_name}"
                    
                    if key not in all_report_data:
                        all_report_data[key] = {
                            "business_unit": business_type,
                            "company": connection_data['tenant_name'],
                            "contact": contact_name,
                            "invoice_details": {}
                        }
                        # Initialize all buckets
                        for bucket_name in bucket_names:
                            all_report_data[key][bucket_name] = 0.0
                            all_report_data[key]["invoice_details"][bucket_name] = []
                    
                    # Process each invoice for this contact
                    for invoice in contact_invoice_list:
                        process_financial_item(
                            item=invoice,
                            report_date=report_date_obj,
                            periods=periods,
                            period_of=period_of,
                            period_type=period_type,
                            bucket_names=bucket_names,
                            report=all_report_data,
                            amount_field="amount_due",
                            date_field="due_date",
                            is_negative=invoice.get('is_negative', False),
                            date_fallback=report_date_obj,
                            connection_name=connection_data['tenant_name'],
                            business_type=business_type,
                            item_type="invoice",
                            show_current=show_current
                        )

                # Process credit notes (apply as negative values for credits)
                for credit_note in credit_notes:
                    process_financial_item(
                        item=credit_note,
                        report_date=report_date_obj,
                        periods=periods,
                        period_of=period_of,
                        period_type=period_type,
                        bucket_names=bucket_names,
                        report=all_report_data,
                        amount_field="remaining_credit",
                        date_field="date",
                        is_negative=True,
                        date_fallback=report_date_obj,
                        connection_name=connection_data['tenant_name'],
                        business_type=connection_data['business_type'],
                        item_type="credit_note",
                        show_current=show_current
                    )
                    
                # Process overpayments (apply as negative values for credits)
                for overpayment in overpayments:
                    process_financial_item(
                        item=overpayment,
                        report_date=report_date_obj,
                        periods=periods,
                        period_of=period_of,
                        period_type=period_type,
                        bucket_names=bucket_names,
                        report=all_report_data,
                        amount_field="remaining_credit",
                        date_field="date",
                        is_negative=True,
                        date_fallback=report_date_obj,
                        connection_name=connection_data['tenant_name'],
                        business_type=connection_data['business_type'],
                        item_type="overpayment",
                        show_current=show_current
                    )
                    
            except Exception as e:
                import traceback
                error_details = traceback.format_exc()
                print(f"[XERO REPORT] Error processing connection {connection_data['tenant_name']} (App {connection_data['app_id']}): {str(e)}")
                print(f"[XERO REPORT] Full error details: {error_details}")
                
                # Track failed connection
                failed_connections.append({
                    "connection_id": connection_data['id'],
                    "tenant_id": connection_data['tenant_id'],
                    "tenant_name": connection_data['tenant_name'],
                    "app_id": connection_data['app_id'],
                    "error": str(e),
                    "error_details": error_details
                })
                
                # Continue with other connections even if one fails
                continue

        # Prepare data for Excel export (only if is_response_only is 0)
        excel_data = []
        print("--------------------------------")
        print("[DEV DEBUG] Looping through all_report_data to prepare data for Excel export")
        print("--------------------------------")
        if is_response_only == 0:
            for key, data in all_report_data.items():
                # Calculate total amount first
                total_amount = 0
                for bucket_name in bucket_names:
                    amount = data.get(bucket_name, 0)
                    total_amount += amount
                contact_name = data.get("contact", "Unknown")
                row = {
                    "Business Unit": data.get("business_unit", "Unknown"),
                    "Company": data.get("company", "Unknown"),
                    "Contact": contact_name
                }
                
                # Add bucket amounts to row
                for bucket_name in bucket_names:
                    amount = data.get(bucket_name, 0)
                    if bucket_name == "Current" and not show_current:
                        # When show_current=False, combine Current and < 1 Month amounts
                        current_amount = data.get("Current", 0)
                        less_than_one_amount = data.get("< 1 Month", 0)
                        row["Current"] = current_amount + less_than_one_amount
                    elif bucket_name == "< 1 Month" and not show_current:
                        # Skip the < 1 Month column when show_current=False since it's combined with Current
                        continue
                    else:
                        row[bucket_name] = amount
                
                row["Total"] = total_amount
                row["Comments"] = ""  # Add blank comments column
                # Generate system comments
                invoice_details = data.get("invoice_details", {})
                
                if not show_current:
                    # When show_current=False, combine Current and < 1 Month invoice details
                    combined_invoice_details = {}
                    current_details = invoice_details.get("Current", [])
                    less_than_one_details = invoice_details.get("< 1 Month", [])
                    combined_invoice_details["Current"] = current_details + less_than_one_details
                    
                    # Add other bucket details
                    for bucket_name in bucket_names:
                        if bucket_name not in ["Current", "< 1 Month"]:
                            combined_invoice_details[bucket_name] = invoice_details.get(bucket_name, [])
                    
                    # Create system bucket names for comments
                    system_bucket_names = ["Current"] + [b for b in bucket_names if b not in ["Current", "< 1 Month"]]
                    
                    # Map display names to data keys for comments
                    bucket_mapping = {}
                    mapped_invoice_details = {}
                    
                    # Create the mapping for display names to data keys
                    for bucket in system_bucket_names:
                        if bucket == "Current":
                            bucket_mapping["Current & < 1 Month"] = "Current"
                        else:
                            bucket_mapping[bucket] = bucket
                    
                    for display_name, data_key in bucket_mapping.items():
                        if data_key in combined_invoice_details:
                            mapped_invoice_details[display_name] = combined_invoice_details[data_key]
                    
                    system_comments = generate_system_comments(mapped_invoice_details, list(mapped_invoice_details.keys()))
                else:
                    system_comments = generate_system_comments(invoice_details, bucket_names)
                row["System Comments"] = system_comments
                # Only include rows that have non-zero amounts
                if total_amount != 0 or any(data.get(bucket_name, 0) != 0 for bucket_name in bucket_names):
                    excel_data.append(row)

        # Handle table format conversion if requested
        if format == 1:
            # Pre-calculate bucket mappings and system bucket names for performance
            system_bucket_names = None
            bucket_mapping = None
            if not show_current:
                system_bucket_names = ["Current"] + [b for b in bucket_names if b not in ["Current", "< 1 Month"]]
                bucket_mapping = {}
                for bucket in system_bucket_names:
                    if bucket == "Current":
                        bucket_mapping["Current & < 1 Month"] = "Current"
                    else:
                        bucket_mapping[bucket] = bucket
            
            # Convert to pandas DataFrame for table format - optimized for large datasets
            table_data = []
            summary_stats = {
                'total_outstanding': 0,
                'companies': set(),
                'contacts': set(),
                'bucket_totals': {bucket: 0 for bucket in bucket_names}
            }
            
            for key, data in all_report_data.items():
                # Calculate total amount efficiently
                total_amount = sum(data.get(bucket_name, 0) for bucket_name in bucket_names)
                
                # Build row efficiently
                row = {
                    "Business Unit": data.get("business_unit", "Unknown"),
                    "Company": data.get("company", "Unknown"),
                    "Contact": data.get("contact", "Unknown")
                }
                
                # Add bucket amounts to row and update summary stats
                for bucket_name in bucket_names:
                    amount = data.get(bucket_name, 0)
                    if bucket_name == "Current" and not show_current:
                        # When show_current=False, combine Current and < 1 Month amounts
                        current_amount = data.get("Current", 0)
                        less_than_one_amount = data.get("< 1 Month", 0)
                        combined_amount = current_amount + less_than_one_amount
                        row["Current"] = combined_amount
                        summary_stats['bucket_totals']["Current"] += combined_amount
                    elif bucket_name == "< 1 Month" and not show_current:
                        # Skip the < 1 Month column when show_current=False since it's combined with Current
                        continue
                    else:
                        row[bucket_name] = amount
                        summary_stats['bucket_totals'][bucket_name] += amount
                
                row["Total"] = total_amount
                
                # Update summary statistics
                summary_stats['total_outstanding'] += total_amount
                summary_stats['companies'].add(data.get("company", "Unknown"))
                summary_stats['contacts'].add(data.get("contact", "Unknown"))
                
                # Generate system comments - optimized
                invoice_details = data.get("invoice_details", {})
                
                if not show_current and system_bucket_names and bucket_mapping:
                    # When show_current=False, combine Current and < 1 Month invoice details
                    combined_invoice_details = {}
                    current_details = invoice_details.get("Current", [])
                    less_than_one_details = invoice_details.get("< 1 Month", [])
                    combined_invoice_details["Current"] = current_details + less_than_one_details
                    
                    # Add other bucket details
                    for bucket_name in bucket_names:
                        if bucket_name not in ["Current", "< 1 Month"]:
                            combined_invoice_details[bucket_name] = invoice_details.get(bucket_name, [])
                    
                    # Map display names to data keys for comments
                    mapped_invoice_details = {}
                    for display_name, data_key in bucket_mapping.items():
                        if data_key in combined_invoice_details:
                            mapped_invoice_details[display_name] = combined_invoice_details[data_key]
                    
                    system_comments = generate_system_comments(mapped_invoice_details, list(mapped_invoice_details.keys()))
                else:
                    system_comments = generate_system_comments(invoice_details, bucket_names)
                row["Invoice Breakdown"] = system_comments ## Changed name from System comments to Invoice Breakdown for Dify clarity
                
                table_data.append(row)
            
            # Create pandas DataFrame
            df = pd.DataFrame(table_data)
            
            # Use pre-calculated summary statistics for better performance
            total_outstanding = summary_stats['total_outstanding']
            companies_count = len(summary_stats['companies'])
            contacts_count = len(summary_stats['contacts'])
            
            # Find highest aging bucket using pre-calculated totals
            highest_aging_bucket = None
            highest_amount = 0
            for bucket, total in summary_stats['bucket_totals'].items():
                if total > highest_amount:
                    highest_amount = total
                    highest_aging_bucket = bucket
            
            # Calculate percentage distribution using pre-calculated totals
            aging_distribution = {}
            for bucket, total in summary_stats['bucket_totals'].items():
                percentage = (total / total_outstanding * 100) if total_outstanding > 0 else 0
                aging_distribution[bucket] = {
                    "amount": round(total, 2),
                    "percentage": round(percentage, 1)
                }
            
            # Prepare response with enhanced table data for Dify
            response_data = {
                "format": "table",
                "data": df.to_dict(orient="records"),
                "columns": df.columns.tolist(),
                "shape": df.shape,
                "generated_at": report_date_obj.isoformat(),
                "report_date": report_date_obj.strftime("%d %B %Y"),
                "total_invoices": total_invoices,
                "summary": {
                    "total_outstanding": round(total_outstanding, 2),
                    "companies_count": companies_count,
                    "contacts_count": contacts_count,
                    "highest_aging_bucket": highest_aging_bucket,
                    "highest_aging_amount": round(highest_amount, 2) if highest_aging_bucket else 0,
                    "aging_distribution": aging_distribution
                },
            }
        else:
            # Prepare JSON response
            response_data = {
                # "aged_receivables": all_report_data,
                "generated_at": report_date_obj.isoformat(),
                "total_invoices": total_invoices,
                "failed_connections": failed_connections,
                "connection_summary": {
                    "total_connections_attempted": len(connections) + len(failed_connections),
                    "successful_connections": len(connections),
                    "failed_connections_count": len(failed_connections),
                    "success_rate": f"{(len(connections) / (len(connections) + len(failed_connections)) * 100):.1f}%" if (len(connections) + len(failed_connections)) > 0 else "0%"
                }
            }
        
        # Generate Excel file only if is_response_only is 0
        if is_response_only == 0:
            try:
                # Define columns for Excel export
                excel_columns = []
                for bucket in bucket_names:
                    if bucket == "Current" and not show_current:
                        # Combine Current and < 1 Month in Excel header
                        excel_columns.append({"header": "Current & < 1 Month", "key": bucket, "width": 15, "format": "currency"})
                    elif bucket == "< 1 Month" and not show_current:
                        # Skip the < 1 Month column when show_current=False since it's combined with Current
                        continue
                    else:
                        excel_columns.append({"header": bucket, "key": bucket, "width": 15, "format": "currency"})
                
                columns = [
                    {"header": "Business Unit", "key": "Business Unit", "width": 20, "format": "text"},
                    {"header": "Company", "key": "Company", "width": 25, "format": "text"},
                    {"header": "Contact", "key": "Contact", "width": 30, "format": "text"},
                    *excel_columns,
                    {"header": "Total", "key": "Total", "width": 15, "format": "currency"},
                    {"header": "Comments", "key": "Comments", "width": 25, "format": "text"},
                    {"header": "System Comments", "key": "System Comments", "width": 60, "format": "text"}
                ]
                
                # Export to Excel - use absolute path to ensure both app and worker use same location
                excel_file_path = export_report_to_excel(
                    data=excel_data,
                    columns=columns,
                    filename="aged_receivables_report",
                    sheet_name="Aged Receivables",
                    title="Aged Receivables Summary",
                    report_date=f"As at {report_date_obj.strftime('%d %B %Y')}",
                    output_dir=os.getenv('OUTPUT_DIR'),
                    include_totals=True,
                    include_percentages=True
                )
                
                response_data["excel_file"] = excel_file_path
                print(f"[AGED_RECEIVABLES] Excel file generated successfully: {excel_file_path}")
                
            except Exception as e:
                import traceback
                error_details = traceback.format_exc()
                print(f"[AGED_RECEIVABLES] Error generating Excel file: {str(e)}")
                print(f"[AGED_RECEIVABLES] Full error details: {error_details}")
                
                # Don't include excel_file in response if generation failed
                response_data["excel_generation_error"] = str(e)
                response_data["excel_generation_details"] = error_details
        
        end_time = datetime.now()
        print("--------------------------------")
        print("[DEV DEBUG] End Time", end_time)
        print("[DEV DEBUG] Time Taken", end_time - start_time)
        print("--------------------------------")

        return response_data
