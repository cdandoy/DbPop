import React, {useEffect, useState} from "react";
import PageHeader from "../pageheader/PageHeader";
import LoadingOverlay from "../utils/LoadingOverlay";
import {CodeDiff, compareTargetToFile} from "../../api/codeApi";
import {Alert} from "react-bootstrap";
import {tableNameToFqName} from "../../models/TableName";
import "./CodeSourceCompare.scss"
import compare_target from "./compare_target.png"

export default function CodeTargetCompare() {
    const [loading, setLoading] = useState(false);
    const [codeDiff, setCodeDiff] = useState<CodeDiff | undefined>();
    const [error, setError] = useState<string | undefined>();

    useEffect(() => {
        setLoading(true);
        setError(undefined);
        compareTargetToFile()
            .then(result => setCodeDiff(result.data))
            .catch((error) => setError(error))
            .finally(() => setLoading(false));
    }, [])

    return <div id={"code-target-compare"}>
        <PageHeader title={"Compare Target"} subtitle={"Compare the tables and sprocs in the Target database with the local SQL files"} tool={<img src={compare_target} style={{width: "20em"}} alt={"image"}/>}/>
        <LoadingOverlay active={loading}/>
        {error && <Alert variant={"danger"}>{error}</Alert>}
        {codeDiff && (
            <div className={"table-container"}>
                <table className={"table table-hover"}>
                    <thead>
                    <tr>
                        <th>Name</th>
                        <th>Status</th>
                    </tr>
                    </thead>
                    <tbody>
                    {codeDiff.entries.slice(0, 100).map(codeDiffEntry => {
                        const fqName = tableNameToFqName(codeDiffEntry.tableName);
                        return (
                            <tr key={fqName}>
                                <td>{fqName}</td>
                                <td>
                                    {codeDiffEntry.databaseTime && codeDiffEntry.fileTime && (codeDiffEntry.databaseTime > codeDiffEntry.fileTime) && "Updated"}
                                    {codeDiffEntry.databaseTime && !codeDiffEntry.fileTime && "Created"}
                                    {!codeDiffEntry.databaseTime && "Deleted"}
                                </td>
                            </tr>
                        )
                    })}
                    </tbody>
                </table>
            </div>
        )}
    </div>
}