from django.shortcuts import render, get_object_or_404
from django.views.generic import ListView, DetailView
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.utils import timezone
from django_tables2 import SingleTableMixin
from django_tables2.export.views import ExportMixin
from .models import MinerviniMetrics, StockPrice, AIAnalysis, Watchlist, TickerDetails, SectorPerformance
from .tables import StockTable
from datetime import datetime, timedelta
import json
import sys
from pathlib import Path

# Import config
try:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    import config
except ImportError:
    config = None


class StockListView(SingleTableMixin, ListView):
    """Main view showing paginated list of stocks"""
    model = MinerviniMetrics
    table_class = StockTable
    template_name = 'stocks/stock_list.html'
    paginate_by = 50
    
    def get_queryset(self):
        """Get queryset with filters applied"""
        # Get latest date with data
        latest_date = MinerviniMetrics.objects.values_list('date', flat=True).order_by('-date').first()
        
        if not latest_date:
            return MinerviniMetrics.objects.none()
        
        queryset = MinerviniMetrics.objects.filter(date=latest_date)
        
        # Apply filters from request
        filter_type = self.request.GET.get('filter', 'all')
        
        if filter_type == 'passing':
            queryset = queryset.filter(passes_minervini=True)
        elif filter_type == 'vcp':
            queryset = queryset.filter(vcp_detected=True)
        elif filter_type == 'stage2':
            queryset = queryset.filter(stage=2)
        elif filter_type == 'stage2_vcp':
            queryset = queryset.filter(stage=2, vcp_detected=True, passes_minervini=True)
        
        # Sort by filter parameter
        sort_by = self.request.GET.get('sort', '-relative_strength')
        queryset = queryset.order_by(sort_by)
        
        return queryset
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        # Get latest date
        latest_date = MinerviniMetrics.objects.values_list('date', flat=True).order_by('-date').first()
        context['latest_date'] = latest_date
        
        # Get filter stats
        if latest_date:
            all_stocks = MinerviniMetrics.objects.filter(date=latest_date)
            context['total_stocks'] = all_stocks.count()
            context['passing_count'] = all_stocks.filter(passes_minervini=True).count()
            context['vcp_count'] = all_stocks.filter(vcp_detected=True).count()
            context['stage2_count'] = all_stocks.filter(stage=2).count()
            context['stage2_vcp_count'] = all_stocks.filter(
                stage=2, vcp_detected=True, passes_minervini=True
            ).count()
        
        context['current_filter'] = self.request.GET.get('filter', 'all')
        
        return context


def dashboard_view(request):
    """Dashboard with summary statistics"""
    # Get latest date
    latest_date = MinerviniMetrics.objects.values_list('date', flat=True).order_by('-date').first()
    
    if not latest_date:
        return render(request, 'stocks/dashboard.html', {'latest_date': None})
    
    # Get all stocks for latest date
    all_stocks = MinerviniMetrics.objects.filter(date=latest_date)
    
    # Calculate statistics
    stats = {
        'total_stocks': all_stocks.count(),
        'passing_minervini': all_stocks.filter(passes_minervini=True).count(),
        'vcp_detected': all_stocks.filter(vcp_detected=True).count(),
        'stage_1': all_stocks.filter(stage=1).count(),
        'stage_2': all_stocks.filter(stage=2).count(),
        'stage_3': all_stocks.filter(stage=3).count(),
        'stage_4': all_stocks.filter(stage=4).count(),
    }
    
    # Top performers
    top_rs = all_stocks.filter(passes_minervini=True).order_by('-relative_strength')[:10]
    top_vcp = all_stocks.filter(vcp_detected=True).order_by('-vcp_score')[:10]
    
    # Best setups (Stage 2 + Minervini + VCP)
    best_setups = all_stocks.filter(
        stage=2,
        passes_minervini=True,
        vcp_detected=True
    ).order_by('-vcp_score', '-relative_strength')[:10]
    
    # Top performing sectors
    top_sectors = SectorPerformance.objects.filter(date=latest_date).order_by('-sector_rs')[:10]
    
    # Stocks with high momentum (positive multi-timeframe returns)
    high_momentum = all_stocks.filter(
        return_1m__gt=0,
        return_3m__gt=0,
        passes_minervini=True
    ).order_by('-return_3m')[:10]
    
    # New 52-week highs
    new_highs = all_stocks.filter(is_52w_high=True, passes_minervini=True).order_by('-relative_strength')[:10]
    
    context = {
        'latest_date': latest_date,
        'stats': stats,
        'top_rs': top_rs,
        'top_vcp': top_vcp,
        'best_setups': best_setups,
        'top_sectors': top_sectors,
        'high_momentum': high_momentum,
        'new_highs': new_highs,
    }
    
    return render(request, 'stocks/dashboard.html', context)


def stock_detail_view(request, symbol):
    """Detail view for a specific stock"""
    # Get latest date
    latest_date = MinerviniMetrics.objects.values_list('date', flat=True).order_by('-date').first()
    
    if not latest_date:
        return render(request, 'stocks/stock_detail.html', {
            'symbol': symbol.upper(),
            'metrics': None,
            'history': [],
            'price_data': []
        })
    
    # Get current metrics
    try:
        current_metrics = MinerviniMetrics.objects.get(symbol=symbol.upper(), date=latest_date)
    except MinerviniMetrics.DoesNotExist:
        current_metrics = None
    
    # Get historical metrics (last 60 days)
    history = MinerviniMetrics.objects.filter(
        symbol=symbol.upper()
    ).order_by('-date')[:60]
    
    # Get historical price data (last 180 days for chart)
    price_data = StockPrice.objects.filter(
        symbol=symbol.upper()
    ).order_by('-date')[:180]
    
    # Reverse to chronological order for chart
    price_data = list(reversed(price_data))
    
    # Prepare chart data as JSON
    chart_data = {
        'dates': [p.date.strftime('%Y-%m-%d') for p in price_data],
        'prices': [float(p.close) if p.close else None for p in price_data],
        'volumes': [int(p.volume) if p.volume else 0 for p in price_data],
        'highs': [float(p.high) if p.high else None for p in price_data],
        'lows': [float(p.low) if p.low else None for p in price_data],
    }
    
    # Get most recent AI analysis for this symbol (any model)
    latest_analysis = None
    try:
        latest_analysis = AIAnalysis.objects.filter(
            symbol=symbol.upper()
        ).order_by('-generated_at').first()
    except Exception as e:
        print(f"Warning: Could not load cached analysis: {e}")
    
    # Get friendly model name if analysis exists
    model_display_name = None
    if latest_analysis:
        model_names = {
            'claude-opus-4-20250514': 'Claude Opus 4.5',
            'claude-sonnet-4-20250514': 'Claude Sonnet 4.5',
            'claude-haiku-4-20250514': 'Claude Haiku 4.5'
        }
        model_display_name = model_names.get(latest_analysis.model_used, latest_analysis.model_used)
    
    # Get ticker details
    ticker_details = None
    try:
        ticker_details = TickerDetails.objects.get(symbol=symbol.upper())
    except TickerDetails.DoesNotExist:
        pass
    except Exception as e:
        print(f"Warning: Could not load ticker details: {e}")
    
    context = {
        'symbol': symbol.upper(),
        'metrics': current_metrics,
        'history': history,
        'latest_date': latest_date,
        'chart_data': json.dumps(chart_data),
        'latest_analysis': latest_analysis,
        'analysis_model_name': model_display_name,
        'ticker_details': ticker_details,
    }
    
    return render(request, 'stocks/stock_detail.html', context)


@require_http_methods(["POST"])
def analyze_stock_ai(request, symbol):
    """
    Generate AI analysis for a stock using Claude API.
    Called via AJAX from the stock detail page.
    """
    if not config or not hasattr(config, 'CLAUDE_API_KEY'):
        return JsonResponse({
            'error': 'API key not configured. Please set CLAUDE_API_KEY in config.py'
        }, status=500)
    
    if config.CLAUDE_API_KEY == "your-api-key-here":
        return JsonResponse({
            'error': 'Please replace the placeholder API key in config.py with your actual Anthropic API key'
        }, status=500)
    
    try:
        from anthropic import Anthropic
    except ImportError:
        return JsonResponse({
            'error': 'Anthropic library not installed. Run: pip install anthropic'
        }, status=500)
    
    # Parse request body to get selected model and force_regenerate flag
    try:
        body = json.loads(request.body) if request.body else {}
        selected_model = body.get('model', 'claude-sonnet-4-20250514')  # Default to Sonnet 4.5
        force_regenerate = body.get('force_regenerate', False)
    except json.JSONDecodeError:
        selected_model = 'claude-sonnet-4-20250514'
        force_regenerate = False
    
    # Validate model selection
    valid_models = [
        'claude-opus-4-20250514',
        'claude-sonnet-4-20250514', 
        'claude-haiku-4-20250514'
    ]
    if selected_model not in valid_models:
        selected_model = 'claude-sonnet-4-20250514'
    
    # Get friendly model name
    model_names = {
        'claude-opus-4-20250514': 'Claude Opus 4.5',
        'claude-sonnet-4-20250514': 'Claude Sonnet 4.5',
        'claude-haiku-4-20250514': 'Claude Haiku 4.5'
    }
    model_display_name = model_names.get(selected_model, selected_model)
    
    # Get latest metrics for the stock
    latest_date = MinerviniMetrics.objects.values_list('date', flat=True).order_by('-date').first()
    
    if not latest_date:
        return JsonResponse({'error': 'No data available'}, status=404)
    
    try:
        metrics = MinerviniMetrics.objects.get(symbol=symbol.upper(), date=latest_date)
    except MinerviniMetrics.DoesNotExist:
        return JsonResponse({'error': f'No data found for {symbol.upper()}'}, status=404)
    
    # Check for cached analysis (unless force_regenerate is True)
    if not force_regenerate:
        try:
            cached_analysis = AIAnalysis.objects.filter(
                symbol=symbol.upper(),
                data_date=latest_date,
                model_used=selected_model
            ).order_by('-generated_at').first()
            
            if cached_analysis:
                # Return cached analysis
                return JsonResponse({
                    'success': True,
                    'analysis': cached_analysis.analysis_text,
                    'symbol': symbol.upper(),
                    'date': str(latest_date),
                    'model': model_display_name,
                    'generated_at': cached_analysis.generated_at.isoformat(),
                    'from_cache': True,
                    'age_hours': round(cached_analysis.age_hours, 1)
                })
        except Exception as e:
            # If cache check fails, continue to generate new analysis
            print(f"Warning: Could not check cache: {e}")
    
    # Get historical data
    history = MinerviniMetrics.objects.filter(
        symbol=symbol.upper()
    ).order_by('-date')[:30]
    
    # Build the prompt with all available data
    prompt = f"""You are a professional stock analyst. Analyze {symbol.upper()} comprehensively by considering:

1. The technical data provided below (Minervini methodology)
2. Any recent news, current events, or market sentiment about this company
3. Recent earnings reports, guidance, or financial announcements
4. Industry trends and competitive landscape
5. Macroeconomic factors that may affect this stock
6. Any other relevant information that could impact the stock's performance

TECHNICAL METRICS (as of {latest_date}):

CURRENT PRICE & STAGE:
- Price: ${metrics.close_price}
- Stage: {metrics.stage} ({metrics.stage_name})
- Relative Strength: {metrics.relative_strength}/100
- Passes Minervini Criteria: {'YES' if metrics.passes_minervini else 'NO'} ({metrics.criteria_passed}/9 criteria)

MOVING AVERAGES:
- 50-day MA: ${metrics.ma_50}
- 150-day MA: ${metrics.ma_150}
- 200-day MA: ${metrics.ma_200}
- 200-day MA Trend (20d): {metrics.ma_200_trend_20d}%

52-WEEK RANGE:
- 52-week High: ${metrics.week_52_high}
- 52-week Low: ${metrics.week_52_low}
- Distance from 52w High: {metrics.percent_from_52w_high}%

VCP (VOLATILITY CONTRACTION PATTERN):
- VCP Detected: {'YES' if metrics.vcp_detected else 'NO'}
- VCP Score: {metrics.vcp_score}/100
- Contraction Count: {metrics.contraction_count}
- Latest Contraction: {metrics.latest_contraction_pct}%
- Volume Contraction: {'YES' if metrics.volume_contraction else 'NO'}
- Pivot Price: ${metrics.pivot_price}

RECENT PRICE TREND (Last 10 days):
"""
    
    # Add historical context
    for i, day in enumerate(history[:10]):
        prompt += f"\n{day.date}: ${day.close_price} | Stage {day.stage} | RS {day.relative_strength}"
    
    prompt += f"""

ANALYSIS REQUIREMENTS:

Provide a comprehensive analysis that includes:

1. **Technical Setup** - Assessment based on Minervini criteria and chart pattern
2. **Fundamental Context** - Recent news, earnings, company developments (if any significant events)
3. **Market Environment** - How current market conditions and sector trends affect this stock
4. **Stage Analysis** - What the current stage means for potential entry/exit
5. **VCP Analysis** - If detected, is it setting up for a breakout?
6. **Catalysts & Risks** - Upcoming events or concerns to watch
7. **Key Price Levels** - Support and resistance levels
8. **Action Recommendation** - Clear BUY, HOLD, WAIT, or AVOID with detailed reasoning

IMPORTANT: 
- If you're aware of any recent news, earnings reports, or significant events for {symbol.upper()}, incorporate them into your analysis
- Consider the broader market context and sector performance
- Be specific about entry points, stop losses, and price targets where applicable
- Keep the analysis actionable and concise (4-5 paragraphs)

Today's date is {datetime.now().strftime('%B %d, %Y')} for context."""
    
    # Call Claude API
    try:
        client = Anthropic(api_key=config.CLAUDE_API_KEY)
        
        message = client.messages.create(
            model=selected_model,
            max_tokens=2000,  # Increased for more comprehensive analysis
            temperature=1.0,  # Allow creative reasoning about news/events
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        analysis = message.content[0].text
        
        # Save analysis to database
        try:
            AIAnalysis.objects.update_or_create(
                symbol=symbol.upper(),
                data_date=latest_date,
                model_used=selected_model,
                defaults={
                    'analysis_text': analysis,
                    'generated_at': timezone.now()
                }
            )
        except Exception as e:
            # Log error but don't fail the request
            print(f"Warning: Could not save analysis to database: {e}")
        
        return JsonResponse({
            'success': True,
            'analysis': analysis,
            'symbol': symbol.upper(),
            'date': str(latest_date),
            'model': model_display_name,
            'generated_at': timezone.now().isoformat(),
            'from_cache': False
        })
        
    except Exception as e:
        return JsonResponse({
            'error': f'Error calling Claude API: {str(e)}'
        }, status=500)


def watchlist_view(request):
    """View showing stocks in the user's watchlist"""
    # Get latest date
    latest_date = MinerviniMetrics.objects.values_list('date', flat=True).order_by('-date').first()
    
    if not latest_date:
        return render(request, 'stocks/watchlist.html', {'latest_date': None, 'stocks': []})
    
    # Get watchlist symbols
    watchlist_symbols = Watchlist.objects.values_list('symbol', flat=True)
    
    # Get metrics for watchlist stocks
    queryset = MinerviniMetrics.objects.filter(
        date=latest_date,
        symbol__in=watchlist_symbols
    ).order_by('-relative_strength')
    
    # Get statistics
    stats = {
        'total_stocks': queryset.count(),
        'passing_minervini': queryset.filter(passes_minervini=True).count(),
        'vcp_detected': queryset.filter(vcp_detected=True).count(),
        'stage_2': queryset.filter(stage=2).count(),
    }
    
    context = {
        'latest_date': latest_date,
        'stocks': queryset,
        'stats': stats,
        'is_watchlist': True,
    }
    
    return render(request, 'stocks/watchlist.html', context)


@require_http_methods(["POST"])
def add_to_watchlist(request, symbol):
    """Add a stock to the watchlist"""
    try:
        watchlist_item, created = Watchlist.objects.get_or_create(
            symbol=symbol.upper()
        )
        return JsonResponse({
            'success': True,
            'added': created,
            'symbol': symbol.upper()
        })
    except Exception as e:
        return JsonResponse({
            'error': str(e)
        }, status=500)


@require_http_methods(["POST"])
def remove_from_watchlist(request, symbol):
    """Remove a stock from the watchlist"""
    try:
        deleted_count = Watchlist.objects.filter(symbol=symbol.upper()).delete()[0]
        return JsonResponse({
            'success': True,
            'removed': deleted_count > 0,
            'symbol': symbol.upper()
        })
    except Exception as e:
        return JsonResponse({
            'error': str(e)
        }, status=500)


@require_http_methods(["GET"])
def check_watchlist_status(request, symbol):
    """Check if a stock is in the watchlist"""
    is_in_watchlist = Watchlist.objects.filter(symbol=symbol.upper()).exists()
    return JsonResponse({
        'in_watchlist': is_in_watchlist,
        'symbol': symbol.upper()
    })


@require_http_methods(["GET"])
def proxy_company_image(request):
    """
    Proxy company branding images from Massive API to hide the API key.
    Reads API key from ~/.massive-api/api_key.txt
    """
    import requests
    from django.http import HttpResponse
    
    image_url = request.GET.get('url')
    
    if not image_url:
        return HttpResponse('Missing URL parameter', status=400)
    
    # Read API key from file
    try:
        api_key_file = Path.home() / ".massive-api" / "api_key.txt"
        if not api_key_file.exists():
            return HttpResponse('API key file not found', status=500)
        
        api_key = api_key_file.read_text().strip()
        
        # Add API key as query parameter
        separator = '&' if '?' in image_url else '?'
        full_url = f"{image_url}{separator}apiKey={api_key}"
        
        # Fetch the image
        response = requests.get(full_url, timeout=5)
        
        if response.status_code == 200:
            # Return the image with proper content type
            content_type = response.headers.get('Content-Type', 'image/png')
            return HttpResponse(response.content, content_type=content_type)
        else:
            return HttpResponse('Image not found', status=404)
            
    except Exception as e:
        print(f"Error proxying image: {e}")
        return HttpResponse('Error fetching image', status=500)


@require_http_methods(["GET"])
def search_stocks(request):
    """Search for stocks by symbol - returns up to 10 results for autocomplete"""
    query = request.GET.get('q', '').strip().upper()
    
    if not query:
        return JsonResponse({'results': []})
    
    # Get latest date
    latest_date = MinerviniMetrics.objects.values_list('date', flat=True).order_by('-date').first()
    
    if not latest_date:
        return JsonResponse({'results': []})
    
    # Search for symbols starting with or containing the query
    # Prioritize symbols that start with the query
    starts_with = MinerviniMetrics.objects.filter(
        date=latest_date,
        symbol__istartswith=query
    ).values('symbol', 'close_price', 'relative_strength', 'stage').order_by('symbol')[:10]
    
    # If we don't have 10 results, also search for symbols containing the query
    results = list(starts_with)
    if len(results) < 10:
        contains = MinerviniMetrics.objects.filter(
            date=latest_date,
            symbol__icontains=query
        ).exclude(
            symbol__istartswith=query
        ).values('symbol', 'close_price', 'relative_strength', 'stage').order_by('symbol')[:10 - len(results)]
        
        results.extend(list(contains))
    
    # Format results
    formatted_results = [
        {
            'symbol': r['symbol'],
            'price': float(r['close_price']) if r['close_price'] else 0,
            'rs': float(r['relative_strength']) if r['relative_strength'] else 0,
            'stage': r['stage']
        }
        for r in results
    ]
    
    return JsonResponse({'results': formatted_results})
