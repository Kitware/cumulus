import datetime as dt

def date_parser(timestring):
    """
        Parse a datetime string from ls -l and return a standard isotime
        
        ls -l returns dates in two different formats:
        
        May  2 09:06 (for dates within 6 months from now)
        May  2 2018  (for dates further away)
        
        Best would be to return ls -l --full-time,
        but unfortunately we have no control over the remote API
    """
    
    recent_time_format = "%b %d %H:%M"
    older_time_format = "%b %d %Y"
    try:
        date = dt.datetime.strptime(timestring, recent_time_format)
        now = dt.datetime.now()
        this_year = dt.datetime(year=now.year,
                           month=date.month, day=date.day,
                           hour=date.hour, minute=date.minute)
        last_year = dt.datetime(year=now.year-1,
                           month=date.month, day=date.day,
                           hour=date.hour, minute=date.minute)
        
        delta_this = abs((now-this_year).total_seconds())
        delta_last = abs((now-last_year).total_seconds())
        if (delta_this > delta_last):
            date = last_year
        else:
            date = this_year
            
    except ValueError:
        try:
            date = dt.datetime.strptime(timestring, older_time_format)
        except ValueError:
            return timestring
    return date.isoformat()

