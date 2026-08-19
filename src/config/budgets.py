"""Оценки длительности внешних вызовов, из которых считаются дедлайны тиков.

Это знание о конкретных адаптерах (zeep и SQLite), а не о домене: числа верны
ровно до тех пор, пока источником остаётся WCF, а хранилищем — SQLite.
"""

WCF_INIT_HTTP_CALLS = 1
WCF_CATALOG_HTTP_CALLS = 2
WCF_STEADY_HTTP_CALLS = 2
WCF_FIRST_TICK_HTTP_CALLS = (
    WCF_INIT_HTTP_CALLS + WCF_CATALOG_HTTP_CALLS + WCF_STEADY_HTTP_CALLS
)

SQLITE_SAVE_BUDGET_S = 2.0
