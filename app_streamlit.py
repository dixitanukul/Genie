import pandas as pd
import streamlit as st
from genie_client import (
    start_conversation,
    poll_until_message_complete,
    execute_query,
    poll_until_statement_done,
    extract_sql_from_message,
    extract_rows_from_result
)

st.set_page_config(page_title="GENIE", layout="wide")
st.title("GENIE")

user_input = st.text_input("Enter your question here")

if st.button("QUERY", use_container_width=True):
    if not user_input.strip():
        st.warning("Please enter a question")
    else:
        with st.spinner("Starting conversation..."):
            conversation_id, message_id = start_conversation(user_input)

        with st.spinner("Thinking..."):
            msg_json = poll_until_message_complete(conversation_id, message_id)

        sql_text = extract_sql_from_message(msg_json)
        if sql_text:
            st.code(sql_text, language="sql")

        attachment_id = msg_json["attachments"][0]["attachment_id"]
        with st.spinner("Executing query..."):
            _ = execute_query(conversation_id, message_id, attachment_id)

        with st.spinner("Fetching results..."):
            result_json = poll_until_statement_done(conversation_id, message_id, attachment_id)

        try:
            col_names, rows = extract_rows_from_result(result_json)
            df = pd.DataFrame(rows, columns=col_names)
            st.dataframe(df, use_container_width=True)
        except KeyError:
            st.error("Unexpected response format while reading results.")
            st.json(result_json)
