import streamlit as st
from tabs.top_games_tab.ui import render_top_games_tab
from tabs.game_tab.ui import render_game_info_tab

def main():
    """Main function to run the Streamlit app"""
    st.set_page_config(page_title="Steam Reviews Analysis", layout="wide")
    
    st.title("Steam Game Reviews Analysis")
    
    # Create tabs
    tab1, tab2 = st.tabs(["Top Games", "Game Info"])
    
    with tab1:
        render_top_games_tab()
    
    with tab2:
        render_game_info_tab()

if __name__ == "__main__":
    main() 