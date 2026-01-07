"""Card components for displaying top items."""

import streamlit as st

from config import USER_COLOURS, TOP_ITEMS_LIMIT


def render_top_items(
    title: str,
    items: list[dict],
    image_key: str,
    primary_text_key: str,
    secondary_text_key: str | None,
    count_key: str,
) -> None:
    """Render a list of top items with image, text, and play count.

    Args:
        title: Section title
        items: List of item dictionaries
        image_key: Key for the image URL in item dict
        primary_text_key: Key for the primary text (bold)
        secondary_text_key: Key for the secondary text (or None)
        count_key: Key for the count/plays value
    """
    st.text(title)
    for item in items[:TOP_ITEMS_LIMIT]:
        with st.container(horizontal=True, border=True):
            inner_cols = st.columns(3, gap=None)
            with inner_cols[0]:
                st.image(item[image_key], width=100)
            with inner_cols[1]:
                st.markdown(f"**{item[primary_text_key]}**")
                if secondary_text_key:
                    st.markdown(f"{item[secondary_text_key]}")
                st.badge(item["user_name"], color=USER_COLOURS[item["user_name"]])
            with inner_cols[2]:
                st.metric(label="plays", value=item[count_key])
