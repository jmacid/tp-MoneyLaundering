RESET = "\033[0m"

RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
MAGENTA = "\033[35m"
CYAN = "\033[36m"


def colorize(text: str, color: str) -> str:
    return f"{color}{text}{RESET}"


def red(text: str) -> str:
    return colorize(text, RED)


def green(text: str) -> str:
    return colorize(text, GREEN)


def yellow(text: str) -> str:
    return colorize(text, YELLOW)


def blue(text: str) -> str:
    return colorize(text, BLUE)


def cyan(text: str) -> str:
    return colorize(text, CYAN)

def magenta(text: str) -> str:
    return colorize(text, MAGENTA)