@router.get("/aged-receivables")
async def get_aged_receivables(
    report_date: str = Query(None, description="Report date in YYYY-MM-DD format"),
    periods: int = Query(4, description="Number of aging periods"),
    period_of: int = Query(1, description="Duration of each period"),
    period_type: str = Query("Month", description="Type of period (Day, Week, Month)"),
    app_id: Optional[int] = Query(None, ge=1, le=2, description="Filter by Xero app ID (1-2)"),
    show_current: bool = Query(True, description="Show Current bucket separately (if false, combines Current and < 1 Month)"),
    aged_receivables_service: XeroAgedReceivablesService = Depends(get_aged_receivables_service),
    xero_auth_service: XeroAuthService = Depends(get_xero_auth_service),
    connection_id: str = Query(None, description="Connection ID(s) - comma-separated for multiple connections"),
    is_response_only: int = Query(1, description="If 1, return response only without Excel generation"),
    format: int = Query(1, description="If 1, return table format; if 0, return JSON format"),
    is_local: bool = Query(False, description="Generate report immediately (true) or queue (false)"),
    is_cache: bool = Query(True, description="Use cache (true) or not (false)"),
    email: str = Query(None, description="Email address to send the report to"),
    db: Session = Depends(get_db)
):
    """
    Custom Aged Receivables report: fetch all unpaid invoices from all connections, 
    group by contact and Xero-style aging bucket, with business unit and company columns.
    Supports multi-app functionality with app_id filtering.
    """
    
    if is_local:
        # Generate report immediately using the service
        return await aged_receivables_service.generate_aged_receivables_report(
            report_date=report_date,
            periods=periods,
            period_of=period_of,
            period_type=period_type,
            app_id=app_id,
            show_current=show_current,
            connection_id=connection_id,
            is_response_only=is_response_only,
            format=format,
            is_cache=is_cache,
            email=email
        )
    else:
        # Queue the job for background processing using database
        queue_service = DatabaseQueueService(db)
        
        job_data = {
            "report_date": report_date,
            "periods": periods,
            "period_of": period_of,
            "period_type": period_type,
            "app_id": app_id,
            "show_current": show_current,
            "connection_id": connection_id,
            "is_cache": is_cache,
            "email": email
        }
        
        job_id = queue_service.enqueue_job("aged_receivables_report", job_data)
        
        return {
            "format": "table",
            "data": [
                {
                    "status": "queued",
                    "job_id": job_id,
                }
            ],
            "columns": ["status", "job_id"],
            "shape": [1, 2],
        }