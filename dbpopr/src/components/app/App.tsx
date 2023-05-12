import './App.scss';
import 'react-bootstrap-typeahead/css/Typeahead.min.css';
import 'react-bootstrap-typeahead/css/Typeahead.bs5.min.css';
import React, {useEffect, useState} from 'react';
import {HashRouter} from "react-router-dom";
import {Header} from "./Header";
import Menu from "./Menu";
import BottomMenu from "./BottomMenu";
import Sidebar from "../sidebar/Sidebar";
import RoutesComponent from "../RoutesComponent";
import {DefaultSiteResponse, siteApi, SiteResponse} from "../../api/siteApi";
import {SiteStatus, useSetupStatusEffect} from "./sitestatus/useSetupStatusEffect";
import SiteStatusComponent from "./sitestatus/SiteStatusComponent";
import useWebSocket from 'react-use-websocket';

export const SiteContext = React.createContext<SiteResponse>(DefaultSiteResponse);

const WS_URL = 'ws://localhost:8080/ws/site';

export default function App() {
    const [siteStatus, setSiteStatus] = useState<SiteStatus>({statuses: [], complete: false});
    const [siteResponse, setSiteResponse] = useState<SiteResponse>(DefaultSiteResponse);

    useEffect(() => {
        siteApi()
            .then(result => setSiteResponse(result.data))
    }, [siteStatus]);

    useSetupStatusEffect(setSiteStatus);

    useWebSocket(WS_URL, {
        onOpen: () => {
            console.log('onOpen');
        },
        onMessage: (a) => {
            console.log("onMessage: " + a.data);
            setSiteResponse(prevState => {
                const ret = {...prevState, message: a.data};
                return ret
            })
        },
        onClose: () => {
            console.log("onClose")
        },
        onError: () => {
            console.log("onError")
        },
        onReconnectStop: () => {
            console.log("onReconnectStop")
        }
    });

    if (siteStatus.complete) {
        return (
            <>
                <HashRouter>
                    <SiteContext.Provider value={siteResponse}>
                        <Sidebar title1={"DbPop"} title2={`v${process.env.REACT_APP_VERSION}`} menu=<Menu/> bottomMenu=<BottomMenu/> >
                            <Header/>
                            <div className="container">
                                <RoutesComponent/>
                            </div>
                        </Sidebar>
                    </SiteContext.Provider>
                </HashRouter>
            </>
        );
    } else {
        return <SiteStatusComponent siteStatus={siteStatus}/>
    }
}
