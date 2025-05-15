import streamlit as st
from src.tabs.top_games_tab.ui import render_top_games_tab
from src.tabs.game_tab.ui import render_game_info_tab
from src.tabs.streaming_tab.ui import render_streaming_tab
from src.utils.ui_utils import create_data_mode_toggle, create_streaming_status_indicators
from src.streaming.config.settings import DASHBOARD_REFRESH_INTERVAL
from src.utils.debug_utils import render_debug_ui, print_debug_summary

# Debug mode flag - set to True to see debug tab
DEBUG_MODE = True

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
        st.caption("System status is updated automatically")
        
        # Add dynamic status indicators for components
        create_streaming_status_indicators()
        
        st.divider()
        st.caption("Made with ❤️ by BigData Team")
    
    # Create tabs (add debug tab if in debug mode)
    if DEBUG_MODE:
        tab1, tab2, tab3, tab4 = st.tabs(["Top Games", "Game Info", "Streaming Analytics", "Debug"])
    else:
        tab1, tab2, tab3 = st.tabs(["Top Games", "Game Info", "Streaming Analytics"])
    
    # Pass data mode to each tab
    with tab1:
        render_top_games_tab(data_mode)
    
    with tab2:
        render_game_info_tab(data_mode)
        
    with tab3:
        render_streaming_tab()
    
    # Render debug tab if in debug mode
    if DEBUG_MODE:
        with tab4:
            render_debug_ui()
            # Also print debug info to console for easier access
            print_debug_summary()

if __name__ == "__main__":
    main() 