import './App.scss';
import 'react-bootstrap-typeahead/css/Typeahead.min.css';
import 'react-bootstrap-typeahead/css/Typeahead.bs5.min.css';
import React, {useState} from 'react';
import {HashRouter} from "react-router-dom";
import {Header} from "./Header";
import Menu from "./Menu";
import BottomMenu from "./BottomMenu";
import {SetupState} from "./SetupState";
import {useSetupStatusEffect} from "./useSetupStatusEffect";
import {SetupStatusComponent} from "../SetupStatusComponent";
import Sidebar from "../sidebar/Sidebar";
import RoutesComponent from "../RoutesComponent";

export default function App() {
    const [setupState, setSetupState] = useState<SetupState>({activity: "Loading", error: null});

    useSetupStatusEffect(setSetupState);

    function Content() {
        if (setupState.activity) {
            return <SetupStatusComponent setupState={setupState}/>
        } else {
            return (
                <div>
                    <RoutesComponent/>
                </div>
            );
        }
    }

    return (
        <>
            <HashRouter>
                <Sidebar title1={"DbPop"} title2={`v${process.env.REACT_APP_VERSION}`} menu=<Menu/> bottomMenu=<BottomMenu/> >
                    <Header/>
                    <div className="container">
                        <Content/>
                    </div>
                </Sidebar>
            </HashRouter>
        </>
    );
}
