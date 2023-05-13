import React, {useContext} from "react";
import {ChangeContext} from "../app/App";
import {Button} from "react-bootstrap";
import './CodeChanges.scss'

export default function CodeChanges() {
    const changeContext = useContext(ChangeContext);

    return <div className={"code-changes-component"}>
        {changeContext.map(change => (
            <div key={change.path} className={"row mt-1"}>
                <div className={"col-5 column-path"}>{change.path}</div>
                <div className={"col-2 column-buttons"}>
                    <Button variant={"primary"} size={"sm"} style={change.databaseChanged ? {} : {visibility: "hidden"}}>
                        <i className={"fa fa-arrow-left"}/>
                    </Button>
                    <Button variant={"primary"} size={"sm"} style={change.fileChanged ? {} : {visibility: "hidden"}}>
                        <i className={"fa fa-arrow-right"}/>
                    </Button>
                </div>
                <div className={"col-5 column-db"}>{change.dbname}</div>
            </div>
        ))
        }
    </div>;
}