import basic
import advanced
import streamlit as st
PAGES = {
    "Basics": basic,
    "Advanced": advanced
}
selection = st.sidebar.radio("Go to page", list(PAGES.keys()))
page = PAGES[selection]


if __name__ == '__main__':
    page.app()
