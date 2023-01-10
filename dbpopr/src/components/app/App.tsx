import './App.scss';
import 'react-bootstrap-typeahead/css/Typeahead.min.css';
import 'react-bootstrap-typeahead/css/Typeahead.bs5.min.css';
import React, {useState} from 'react';
import {HashRouter, Route, Routes} from "react-router-dom";
import DatasetDetails from "../DatasetDetails";
import Dashboard from "../dashboard/Dashboard"
import AddData from "../AddData";
import VirtualFksComponent from "../vfk/VirtualFksComponent";
import EditVirtualFkComponent from "../vfk/EditVirtualFkComponent";
import {Header} from "./Header";
import Menu from "./Menu";
import BottomMenu from "./BottomMenu";
import {SetupState} from "./SetupState";
import {useSetupStatusEffect} from "./useSetupStatusEffect";
import {SetupStatusComponent} from "../SetupStatusComponent";
import Sidebar from "../sidebar/Sidebar";
import Datasets from "../datasets/Datasets";

export default function App() {
    const [setupState, setSetupState] = useState<SetupState>({activity: "Loading", error: null});

    useSetupStatusEffect(setSetupState);

    function Content() {
        if (setupState.activity) {
            return <SetupStatusComponent setupState={setupState}/>
        } else {
            return (
                <div>
                    <Routes>
                        <Route path="/dataset/:dataset" element=<DatasetDetails/>/>
                        <Route path="/add/:datasetName" element=<AddData/>/>
                        <Route path="/datasets" element=<Datasets/>/>
                        <Route path="/vfk" element=<VirtualFksComponent/>/>
                        <Route path="/vfk/add" element=<EditVirtualFkComponent/>/>
                        <Route path="/vfk/:pkTable/:fkName" element=<EditVirtualFkComponent/>/>
                        <Route path="/" element=<Dashboard/>/>
                    </Routes>
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
