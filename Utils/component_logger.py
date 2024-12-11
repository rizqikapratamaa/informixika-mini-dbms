RESET = "\033[0m"
BLACK = "\033[30m"
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
MAGENTA = "\033[35m"
CYAN = "\033[36m"
WHITE = "\033[37m"
ORANGE = "\033[38;5;208m"
PURPLE = "\033[38;5;93m"
BROWN = "\033[38;5;94m"
PINK = "\033[38;5;205m"
LIME = "\033[38;5;10m"
TEAL = "\033[38;5;14m"
NAVY = "\033[38;5;19m"
LIGHT_BLUE = "\033[38;5;153m"
LIGHT_BROWN = "\033[38;5;137m"

COMPONENT_COLORS = {
    "QP": LIGHT_BLUE,
    "QO": YELLOW,
    "CCM": LIGHT_BROWN,
    "SM": PINK,
    "FRM": GREEN,
    "Socket": ORANGE,
}

MAX_COMPONENT_LENGTH = max(len(component) for component in COMPONENT_COLORS)


def center_text(text: str, width: int) -> str:
    return text.center(width)


def log(component: str, message: str, log_type: str = "info"):
    if component not in COMPONENT_COLORS:
        raise ValueError(f"Unknown component: {component}")

    color = COMPONENT_COLORS[component]
    centered_component = center_text(component, MAX_COMPONENT_LENGTH)
    prefix = f"{color}[{centered_component}]{RESET}"
    full_message = f"{prefix} {message}{RESET}"
    print(full_message)


def log_qp(message: str, log_type: str = "info"):
    log("QP", message, log_type)


def log_qo(message: str, log_type: str = "info"):
    log("QO", message, log_type)


def log_ccm(message: str, log_type: str = "info"):
    log("CCM", message, log_type)


def log_sm(message: str, log_type: str = "info"):
    log("SM", message, log_type)


def log_frm(message: str, log_type: str = "info"):
    log("FRM", message, log_type)


def log_socket(message: str, log_type: str = "info"):
    log("Socket", message, log_type)
