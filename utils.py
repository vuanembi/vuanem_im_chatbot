import re

def format_value(value):
    if value is None:
        return None
    elif value >= 1e9:
        return f"{value/1e9:.2f} B"
    elif value >= 1e6:
        return f"{value/1e6:.2f} M"
    elif value >= 1e3:
        return f"{value/1e3:.2f} K"
    elif value <= 1 and value >= -1:
        return f"{value*1e2:.2f} %"
    elif isinstance(value, int):
        return f"{value}"
    else:
        return f"{value:.2f}"

def format_metric_name(self):
    pattern = "ASM.+(?=[A-Z])"
    self.name = re.sub(pattern, "", self.name)
