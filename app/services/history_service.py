import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

def get_session_history(session_id: str) -> List[Dict]:
    import app.app_state as app_state
    state = app_state.get_session(session_id)
    return state.chat_history

def clear_session_history(session_id: str):
    import app.app_state as app_state
    state = app_state.get_session(session_id)
    state.chat_history = []
    logger.info(f"Chat history cleared for session {session_id}")
