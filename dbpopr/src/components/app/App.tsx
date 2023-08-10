import './App.scss';
import 'react-bootstrap-typeahead/css/Typeahead.min.css';
import 'react-bootstrap-typeahead/css/Typeahead.bs5.min.css';
import React from 'react';
import {HashRouter} from "react-router-dom";
import {Header} from "./Header";
import Menu from "./Menu";
import BottomMenu from "./BottomMenu";
import Sidebar from "../sidebar/Sidebar";
import RoutesComponent from "../RoutesComponent";
import {useWebSocketState, WebSocketStateContext} from "../ws/useWebSocketState";
import {ServerDisconnected} from "./ServerDisconnected";


export default function App() {
    const messageState = useWebSocketState();

    return (
        <>
            <HashRouter>
                <WebSocketStateContext.Provider value={messageState}>
                    <Sidebar title1={"DbPop"} title2={`v${process.env.REACT_APP_VERSION}`} menu=<Menu/> bottomMenu=<BottomMenu/> >
                        <Header/>
                        <div id="content-container">
                            <RoutesComponent/>
                        </div>
                    </Sidebar>
                    {messageState.connected || <ServerDisconnected/>}
                </WebSocketStateContext.Provider>
            </HashRouter>
        </>
    );
}
