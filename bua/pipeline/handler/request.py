from typing import Dict


class HandlerRequest:

    def __init__(self, event: Dict, step: Dict, data: Dict):
        self.event = event
        self.step = step
        self.data = data
