import streamlit as st
import requests
import json
import uuid
import pandas as pd
from datetime import datetime

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Revenue Intelligence Chat",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ──────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display&family=DM+Sans:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap');

/* ── Root tokens ── */
:root {
    --bg:       #0d0f14;
    --surface:  #141720;
    --card:     #1b1f2e;
    --border:   #252a3a;
    --gold:     #c9a84c;
    --gold-dim: #8a6f2e;
    --text:     #e8e4dc;
    --muted:    #6b7280;
    --accent:   #4f6ef7;
    --green:    #34d399;
    --red:      #f87171;
}

/* ── Global resets ── */
html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
    background-color: var(--bg);
    color: var(--text);
}

.stApp { background: var(--bg); }

/* hide default Streamlit chrome */
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding: 1.5rem 2rem 2rem; max-width: 100%; }

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background: var(--surface);
    border-right: 1px solid var(--border);
}
section[data-testid="stSidebar"] .block-container { padding: 1.5rem 1rem; }

/* ── Chat messages ── */
.chat-wrapper { display: flex; flex-direction: column; gap: 1rem; margin-bottom: 1.5rem; }

.msg-user {
    align-self: flex-end;
    background: linear-gradient(135deg, #2a3270, #1a2060);
    border: 1px solid #3a45a0;
    border-radius: 18px 18px 4px 18px;
    padding: 0.75rem 1.1rem;
    max-width: 70%;
    font-size: 0.95rem;
    line-height: 1.5;
    box-shadow: 0 4px 20px rgba(79,110,247,0.15);
}

.msg-bot {
    align-self: flex-start;
    background: var(--card);
    border: 1px solid var(--border);
    border-left: 3px solid var(--gold);
    border-radius: 4px 18px 18px 18px;
    padding: 0.85rem 1.1rem;
    max-width: 80%;
    font-size: 0.95rem;
    line-height: 1.6;
    box-shadow: 0 4px 20px rgba(0,0,0,0.3);
}

.msg-label {
    font-size: 0.7rem;
    font-weight: 600;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    margin-bottom: 0.35rem;
}

.msg-user .msg-label  { color: #8fa4ff; text-align: right; }
.msg-bot  .msg-label  { color: var(--gold); }

/* ── Meta pills ── */
.meta-row { display: flex; flex-wrap: wrap; gap: 0.4rem; margin-top: 0.6rem; }
.pill {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.67rem;
    padding: 0.2rem 0.55rem;
    border-radius: 999px;
    border: 1px solid var(--border);
    background: var(--surface);
    color: var(--muted);
}
.pill.green { border-color: #134e38; color: var(--green); background: #0b2e22; }
.pill.blue  { border-color: #1e3a6e; color: #93b4ff;      background: #0f1f42; }
.pill.gold  { border-color: #4a3a1a; color: var(--gold);  background: #2a2010; }

/* ── SQL block ── */
.sql-block {
    margin-top: 0.75rem;
    background: #0a0c12;
    border: 1px solid var(--border);
    border-radius: 8px;
    overflow: hidden;
}
.sql-header {
    background: var(--border);
    padding: 0.3rem 0.75rem;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.65rem;
    color: var(--muted);
    letter-spacing: 0.08em;
    text-transform: uppercase;
}
.sql-body {
    padding: 0.75rem;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.72rem;
    color: #a5d6a7;
    white-space: pre-wrap;
    word-break: break-all;
    line-height: 1.6;
}

/* ── Hero title ── */
.hero-title {
    font-family: 'DM Serif Display', serif;
    font-size: 1.6rem;
    background: linear-gradient(90deg, var(--gold) 0%, #e8d5a0 50%, var(--gold) 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    line-height: 1.2;
    margin-bottom: 0.2rem;
}
.hero-sub {
    font-size: 0.8rem;
    color: var(--muted);
    letter-spacing: 0.05em;
}

/* ── Input area ── */
.stTextInput > div > div > input {
    background: var(--card) !important;
    border: 1px solid var(--border) !important;
    border-radius: 12px !important;
    color: var(--text) !important;
    font-family: 'DM Sans', sans-serif !important;
    font-size: 0.95rem !important;
    padding: 0.75rem 1rem !important;
}
.stTextInput > div > div > input:focus {
    border-color: var(--gold) !important;
    box-shadow: 0 0 0 3px rgba(201,168,76,0.12) !important;
}

/* ── Buttons ── */
.stButton > button {
    background: linear-gradient(135deg, var(--gold-dim), var(--gold)) !important;
    color: #0d0f14 !important;
    font-weight: 600 !important;
    border: none !important;
    border-radius: 10px !important;
    padding: 0.55rem 1.4rem !important;
    font-size: 0.9rem !important;
    transition: all 0.2s ease !important;
}
.stButton > button:hover {
    transform: translateY(-1px) !important;
    box-shadow: 0 6px 20px rgba(201,168,76,0.35) !important;
}

/* ── Spinner ── */
.stSpinner > div { border-top-color: var(--gold) !important; }

/* ── Divider ── */
hr { border-color: var(--border) !important; margin: 0.75rem 0 !important; }

/* ── Scrollbar ── */
::-webkit-scrollbar       { width: 5px; }
::-webkit-scrollbar-track { background: var(--bg); }
::-webkit-scrollbar-thumb { background: var(--border); border-radius: 999px; }

/* ── No-data state ── */
.empty-state {
    text-align: center;
    padding: 3rem 1rem;
    color: var(--muted);
}
.empty-state .icon { font-size: 2.5rem; margin-bottom: 0.5rem; }
.empty-state .hint { font-size: 0.82rem; margin-top: 0.3rem; }
</style>
""", unsafe_allow_html=True)

# ── Session state ──────────────────────────────────────────────────────────────
if "messages" not in st.session_state:
    st.session_state.messages = []
if "session_id" not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())
if "show_sql" not in st.session_state:
    st.session_state.show_sql = True

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown('<div class="hero-title">Revenue Intel</div>', unsafe_allow_html=True)
    st.markdown('<div class="hero-sub">Property Analytics Assistant</div>', unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    api_url = st.text_input("API Endpoint", value="http://localhost:8000/chat", key="api_url")
    st.markdown("<hr>", unsafe_allow_html=True)

    st.markdown("**Display Options**")
    st.session_state.show_sql = st.toggle("Show SQL Queries", value=st.session_state.show_sql)

    st.markdown("<hr>", unsafe_allow_html=True)

    st.markdown("**Session**")
    sid_display = st.session_state.session_id[:8] + "…"
    st.markdown(f'<span class="pill blue">ID: {sid_display}</span>', unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("🗑  Clear Chat", use_container_width=True):
        st.session_state.messages = []
        st.session_state.session_id = str(uuid.uuid4())
        st.rerun()

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("""
    <div style="font-size:0.72rem; color:#6b7280; line-height:1.6;">
    <b style="color:#9ca3af;">Sample queries</b><br>
    • Property-wise rent revenue 2024–2026<br>
    • Total collections this month<br>
    • Overdue payments by tenant<br>
    • Top 5 revenue properties
    </div>
    """, unsafe_allow_html=True)

# ── Helpers ────────────────────────────────────────────────────────────────────
def call_api(question: str, session_id: str) -> dict:
    payload = {"question": question, "session_id": session_id}
    response = requests.post(
        st.session_state.api_url,
        headers={"accept": "application/json", "Content-Type": "application/json"},
        data=json.dumps(payload),
        timeout=30,
    )
    response.raise_for_status()
    return response.json()

def render_pill(text, cls=""):
    return f'<span class="pill {cls}">{text}</span>'

def render_message(role: str, content: dict):
    if role == "user":
        st.markdown(f"""
        <div class="msg-user">
            <div class="msg-label">You</div>
            {content["text"]}
        </div>
        """, unsafe_allow_html=True)
    else:
        answer = content.get("answer", "—")
        strategy = content.get("strategy", "")
        sql_query = content.get("sql_query", "")
        row_count = content.get("sql_row_count", 0)
        attempts = content.get("sql_attempts", "")
        success = content.get("success", False)

        success_pill = render_pill("✓ Success", "green") if success else render_pill("✗ Failed", "")
        strategy_pill = render_pill(f"⚙ {strategy}", "blue") if strategy else ""
        rows_pill = render_pill(f"↩ {row_count} rows", "gold") if row_count is not None else ""
        attempts_pill = render_pill(f"↻ {attempts} attempt(s)", "") if attempts else ""

        sql_html = ""
        if st.session_state.show_sql and sql_query:
            sql_html = f"""
            <div class="sql-block">
                <div class="sql-header">SQL — generated query</div>
                <div class="sql-body">{sql_query}</div>
            </div>
            """

        st.markdown(f"""
        <div class="msg-bot">
            <div class="msg-label">Assistant</div>
            <div>{answer}</div>
            <div class="meta-row">
                {success_pill}{strategy_pill}{rows_pill}{attempts_pill}
            </div>
            {sql_html}
        </div>
        """, unsafe_allow_html=True)

        # If there's tabular data in the answer, try to render it
        if content.get("table_data"):
            df = pd.DataFrame(content["table_data"])
            st.dataframe(
                df,
                use_container_width=True,
                hide_index=True,
            )

# ── Main layout ────────────────────────────────────────────────────────────────
col_chat, col_pad = st.columns([3, 1])

with col_chat:
    st.markdown("### 💬 Chat")

    # Messages
    chat_container = st.container()
    with chat_container:
        if not st.session_state.messages:
            st.markdown("""
            <div class="empty-state">
                <div class="icon">📊</div>
                <div>Start by asking a revenue or property question.</div>
                <div class="hint">e.g. "Give property-wise rent revenue for 2024 to 2026"</div>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown('<div class="chat-wrapper">', unsafe_allow_html=True)
            for msg in st.session_state.messages:
                render_message(msg["role"], msg["content"])
            st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Input row
    input_col, btn_col = st.columns([5, 1])
    with input_col:
        user_input = st.text_input(
            "Ask a question…",
            placeholder="e.g. Give property-wise rent revenue for 2024 to 2026",
            label_visibility="collapsed",
            key="user_input",
        )
    with btn_col:
        send = st.button("Send →", use_container_width=True)

    # Handle send
    if send and user_input.strip():
        question = user_input.strip()

        # Append user message
        st.session_state.messages.append({
            "role": "user",
            "content": {"text": question},
        })

        # Call API
        with st.spinner("Querying…"):
            try:
                result = call_api(question, st.session_state.session_id)
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": result,
                })
            except requests.exceptions.ConnectionError:
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": {
                        "answer": "⚠️ Could not connect to the API. Make sure the server is running at **" + st.session_state.api_url + "**.",
                        "success": False,
                    },
                })
            except requests.exceptions.Timeout:
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": {
                        "answer": "⚠️ Request timed out. The server took too long to respond.",
                        "success": False,
                    },
                })
            except Exception as e:
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": {
                        "answer": f"⚠️ Unexpected error: {str(e)}",
                        "success": False,
                    },
                })

        st.rerun()

with col_pad:
    st.markdown("""
    <div style="padding: 1rem; background: #141720; border: 1px solid #252a3a; border-radius: 12px; margin-top: 2.5rem;">
        <div style="font-family:'DM Serif Display',serif; font-size:1rem; color:#c9a84c; margin-bottom:0.6rem;">Response Fields</div>
        <div style="font-size:0.75rem; color:#9ca3af; line-height:2;">
            <b style="color:#e8e4dc;">answer</b> — Natural language reply<br>
            <b style="color:#e8e4dc;">strategy</b> — Execution path used<br>
            <b style="color:#e8e4dc;">sql_query</b> — Generated SQL<br>
            <b style="color:#e8e4dc;">sql_row_count</b> — Rows returned<br>
            <b style="color:#e8e4dc;">sql_attempts</b> — Retry count<br>
            <b style="color:#e8e4dc;">success</b> — Query status
        </div>
    </div>
    """, unsafe_allow_html=True)