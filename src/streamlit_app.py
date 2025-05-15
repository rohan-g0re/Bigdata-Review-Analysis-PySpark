import streamlit as st
from src.tabs.top_games_tab.ui import render_top_games_tab
from src.tabs.game_tab.ui import render_game_info_tab
from src.tabs.streaming_tab.ui import render_streaming_tab
from src.utils.ui_utils import create_data_mode_toggle
from src.streaming.config.settings import DASHBOARD_REFRESH_INTERVAL

def main():
    """Main function to run the Streamlit app"""
    st.set_page_config(
        page_title="Steam Reviews Analysis", 
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("Steam Game Reviews Analysis")
    
    # Add mode toggle at the top
    with st.sidebar:
        st.title("Dashboard Settings")
        data_mode = create_data_mode_toggle()
        
        if data_mode == "streaming":
            st.info("Real-time Streaming Mode: Displaying data from the Kafka streaming pipeline")
            st.caption(f"Auto-refresh interval: {DASHBOARD_REFRESH_INTERVAL} seconds")
        else:
            st.info("Batch Analysis Mode: Analyzing data directly from Parquet files")
        
        # Add system status indicator
        st.subheader("System Status")
        st.caption("Start components with run_system.py")
        
        # Add status indicators for components
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Producer**")
            st.markdown("**Consumer**")
        with col2:
            # These would ideally be dynamically updated based on system status
            st.markdown("⚪ Unknown")
            st.markdown("⚪ Unknown")
        
        st.divider()
        st.caption("Made with ❤️ by BigData Team")
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs(["Top Games", "Game Info", "Streaming Analytics"])
    
    # Pass data mode to each tab
    with tab1:
        render_top_games_tab(data_mode)
    
    with tab2:
        render_game_info_tab(data_mode)
        
    with tab3:
        render_streaming_tab()

if __name__ == "__main__":
    main() 