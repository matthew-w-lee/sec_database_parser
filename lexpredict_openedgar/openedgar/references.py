balance_sheet_terms = [
    "^\s*consolidated\s+balance\s+sheets\s*$",
    "^balance sheets\s*$",
    "^CONSOLIDATED BALANCE SHEETS\s*$",
    "^\s*consolidated\s+balance\s+sheet\s+[(]continued[)]\s*$",
    "^consolidated\s+statement\s+of\s+financial\s+position\s*$",
    "^\s*consolidated\s+balance\s+sheet[s]?\s*(December 31,)?\s*$",
    "^\s*{consolidated\s+balance\s+sheet[s]?}\s*$",
    "^\s*consolidated\s+balance\s+sheet[s]?\s+35\s*$",
]

income_statement_terms = [
    "^consolidated\s+statements\s+of\s+operations\s*$",
    "^consolidated\s+statements\s+of\s+income\s*$",
    "^consolidated\s+statement\s+of\s+operations\s*$",
    "^consolidated\s+income\s+statements\s*$",
    "^statements of income\s*$",
    "^income statements\s*$",
    "^consolidated\s+statements\s+of\s+earnings\s*$",
    "^\s*consolidated\s+income\s+statement\s*$",
    "^\s*consolidated\s+statements\s+of\s+earnings\s*$",
    "^\s*consolidated\s+profit\s+and\s+loss\s+account\s*$",
    "^\s*consolidated\s+statement\s+of\s+income\s*$",
    "^\s*consolidated\s+statements\s+of\s+income\s+and\s+comprehensive\s+income\s*$",
    "^\s*consolidated\s+statements\s+of\s+income\s+[(]loss[)]\s+and\s+comprehensive\s+income\s+[(]loss[)]\s*$",
    "^\s*consolidated\s+statements\s+of\s+comprehensive\s+income\s*$",
    "^\s*Consolidated Statements of Income\s*$",
    "^\s*{Consolidated Statements of Income}\s*$",
    "^\s*Consolidated Statements of Income\s*For the years ended December 31,",
    "^\s*Consolidated Statements of Income\s+34\s*$",
]

equity_statement_terms = [
    "^consolidated\s+statements\s+of\s+changes\s+in\s+stockholders\s*$",
    "^consolidated\s+statements\s+of\s+stockholders\s*$",
    "^consolidated\s+statements\s+of\s+changes\s+in\s+equity\s*$",
    "^statements\s+of\s+shareholders.\s+equity\s*$",
    "^consolidated\s+statements\s+of\s+shareholders.\s+equity\s*$",
    "^stockholders.\s+equity\s+statements\s*$",
    "^Consolidated Statements of Redeemable Noncontrolling Interests and Equity\s*$",
    "^Consolidated Statement of Changes in Equity\s*$",
    "^consolidated\s+statements\s+of\s+equity\s*$",
    "^consolidated\s+statements\s+of\s+changes\s+in\s+equity\s*$",
    "^Consolidated Statements of Changes in Shareholders. Equity\s*$",
    "^CONSOLIDATED STATEMENTS OF CHANGES IN EQUITY\s*$",
    "^\s*stockholders.\s+equity\s+statements\s*$",
    "^\s*consolidated\s+statement\s+of\s+changes\s+in\s+equity\s*$",
    "^\s*consolidated\s+statement\s+of\s+changes\s+in\s+shareholders.\s+equity\s*$",
    "^\s*consolidated\s+statements\s+of\s+changes\s+in\s+stockholders.\s+equity\s*$"
]

comprehensive_income_terms = [
    "^consolidated\s+statements\s+of\s+comprehensive\s+income\s*$",
    "^consolidated\s+statements\s+of\s+comprehensive\s+\(loss\)\s+income\s*$",
    "^comprehensive\s+income\s+statements\s*$",
    "^consolidated\s+statements\s+of\s+comprehensive\s+loss\s*$",
    "^Consolidated Statements of Comprehensive Loss\s*$",
    "^Consolidated Statement of Comprehensive Income\s*$",
    "^/s*consolidated\s+statement\s+of\s+recognized\s+income\s+and\s+expense\s*$",
    "^/s*consolidated\s+statement\s+of\s+total\s+recognized\s+gains\s+and\s+losses\s*$",
    "^/s*consolidated\s+statements\s+of\s+comprehensive\s+income\s+[(]loss[)]\s*$"

]

cash_flow_statement_terms = [
    "^consolidated\s+statements\s+of\s+cash\s+flows\s*$",
    "^consolidated\s+statements\s+of\s+cash\s+flows\s*[(]*\s*continued[)]*\s*$",
    "^\s*cash\s+flows\s+statements\s*$",
    "^\s*consolidated\s+statement\s+of\s+cash\s+flows\s*$",
    "^\s*consolidated\s+cash\s+flow\s+statement\s*$"
]