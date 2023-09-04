import React, {useContext, useEffect, useState} from "react";
import {Button} from "react-bootstrap";
import './CodeChanges.scss'
import PageHeader from "../pageheader/PageHeader";
import {ObjectIdentifier} from "../../models/ObjectIdentifier";
import {WebSocketStateContext} from "../ws/useWebSocketState";
import {NavLink} from "react-router-dom";
import axios from "axios";

function Message({message}: { message: string }) {
    return <>
        <h5 className={"text-center"}>
            {message}
        </h5>
    </>
}

export interface ChangedObject {
    objectIdentifier: ObjectIdentifier;
    name: string;
    type: string;
    changeType: string;
}

export default function CodeChanges() {
    const siteStatus = useContext(WebSocketStateContext);
    const [changedObjects, setChangedObjects] = useState<ChangedObject[]>([]);
    const [refreshCount, setRefreshCount] = useState(0)

    useEffect(() => {
        axios.get<ChangedObject[]>(`/codechanges/target`)
            .then(response => {
                setChangedObjects(response.data);
            });
    }, [siteStatus.codeDiffChanges, refreshCount]);

    function refresh() {
        setRefreshCount(refreshCount + 1);
    }

    function onApplyFileChange(objectIdentifier: ObjectIdentifier) {
        axios.post(`/codechanges/target/apply-file`, objectIdentifier)
            .then(refresh);
    }

    function onApplyDbChange(objectIdentifier: ObjectIdentifier) {
        axios.post(`/codechanges/target/apply-db`, objectIdentifier)
            .then(refresh);
    }

    function onApplyAllFileChanges() {
        axios.post(`/codechanges/target/apply-all-files`)
            .then(refresh);
    }

    function onApplyAllDbChanges() {
        axios.post(`/codechanges/target/apply-all-db`)
            .then(refresh);
    }

    function ObjectIdentifierIcon({objectIdentifier}: { objectIdentifier: ObjectIdentifier }) {
        function icon(type: string) {
            if (type === 'SQL_STORED_PROCEDURE') return 'fa-code'
            if (type === 'USER_TABLE') return 'fa-table'
            return ''
        }

        function title(type: string) {
            if (type === 'SQL_STORED_PROCEDURE') return 'Stored Procedure'
            if (type === 'USER_TABLE') return 'Table'
            return ''
        }

        return <>
            <i className={`fa fa-fw ${icon(objectIdentifier.type)}`} title={title(objectIdentifier.type)}></i>&nbsp;
        </>
    }

    function Footer() {
        // If there is only one change, no need for an "Apply All"
        if (changedObjects.length <= 1) return <></>

        // We can only apply-all when there are updates, not when the SQL is only in files or only in database
        if (changedObjects.filter(change => change.changeType !== "UPDATED").length > 0) {
            return <></>
        }

        return <tfoot>
        <tr>
            <td style={{width: "100%"}} className={"object-name"}>
                <strong>All</strong>
            </td>
            <td style={{minWidth: '20em'}} className={"text-center"}>
                <Button variant={"primary"}
                        size={"sm"}
                        className={"apply-button"}
                        onClick={() => onApplyAllFileChanges()}
                >
                    Upload All <i className={"fa fa-arrow-right"}/>
                </Button>
            </td>
            <td style={{minWidth: '20em'}} className={"text-center"}>
                <Button variant={"primary"}
                        size={"sm"}
                        className={"apply-button"}
                        onClick={() => onApplyAllDbChanges()}
                >
                    <i className={"fa fa-arrow-left"}/> Download All
                </Button>
            </td>
        </tr>
        </tfoot>
    }

    function ChangesBox() {

        function Changes({change}: { change: ChangedObject }) {
            const objectIdentifier = change.objectIdentifier;

            return <>
                <td style={{minWidth: '20em'}} className={"text-center"}>
                    {change.changeType === "FILE_ONLY" &&
                        <Button variant={"danger"}
                                size={"sm"}
                                className={"apply-button"}
                                onClick={() => onApplyDbChange(objectIdentifier)}
                        >
                            <i className={"fa fa-trash"}/> Delete
                        </Button>
                    }
                    {(change.changeType === "FILE_ONLY" || change.changeType === "UPDATED") &&
                        <Button variant={"primary"}
                                size={"sm"}
                                className={"apply-button"}
                                onClick={() => onApplyFileChange(objectIdentifier)}
                        >
                            Upload <i className={"fa fa-arrow-right"}/>
                        </Button>
                    }
                </td>
                <td style={{minWidth: '20em'}} className={"text-center"}>
                    {(change.changeType === "DATABASE_ONLY" || change.changeType === "UPDATED") &&
                        <Button variant={"primary"}
                                size={"sm"}
                                className={"apply-button"}
                                onClick={() => onApplyDbChange(objectIdentifier)}
                        >
                            <i className={"fa fa-arrow-left"}/> Download
                        </Button>
                    }
                    {change.changeType === "DATABASE_ONLY" &&
                        <Button variant={"danger"}
                                size={"sm"}
                                className={"apply-button"}
                                onClick={() => onApplyFileChange(objectIdentifier)}
                        >
                            <i className={"fa fa-trash"}/> Drop
                        </Button>
                    }
                </td>
            </>
        }

        function changeRow(change: ChangedObject) {
            return <tr key={change.objectIdentifier.type + '-' + change.name}>
                <td width={"80%"} className={"object-name"}>
                    <NavLink to={"/codechanges/diff"} state={change.objectIdentifier}>
                        <ObjectIdentifierIcon objectIdentifier={change.objectIdentifier}/>
                        {change.name}
                    </NavLink>
                </td>
                <Changes change={change}/>
            </tr>
        }

        return <>
            <table className={"table table-hover"}>
                <thead>
                <tr>
                    <th>Object</th>
                    <th style={{minWidth: '20em'}} className={'text-center'}>File</th>
                    <th style={{minWidth: '20em'}} className={'text-center'}>Database</th>
                </tr>
                </thead>
                <tbody>
                {changedObjects.map(changeRow)}
                {changedObjects.length >= 100 && <tr>
                    <td colSpan={2}><Message message={"Too many changes to display"}/></td>
                </tr>
                }
                </tbody>
                <Footer/>
            </table>
        </>
    }

    function Scanning() {
        return <h5 className={"text-center"}>
            <i className={"fa fa-spinner fa-spin"}/> Scanning
        </h5>
    }

    function NoChanges() {
        return <Message message={"No Changes Detected"}/>;
    }

    function Content() {
        if (!siteStatus.codeScanComplete) return <Scanning/>
        if (changedObjects.length === 0) return <NoChanges/>
        return <ChangesBox/>
    }

    return <div className={"code-changes-component container"}>
        <PageHeader title={"Code Changes"} subtitle={"Track modified database code"}/>
        <Content/>
    </div>
}