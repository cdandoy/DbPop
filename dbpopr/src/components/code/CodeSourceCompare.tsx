import React, {useEffect, useState} from "react";
import PageHeader from "../pageheader/PageHeader";
import LoadingOverlay from "../utils/LoadingOverlay";
import {CodeDiff, compareSourceToFile} from "../../api/codeApi";
import {Alert} from "react-bootstrap";
import {tableNameToFqName} from "../../models/TableName";
import "./CodeSourceCompare.scss"

export default function CodeSourceCompare() {
    const [loading, setLoading] = useState(false);
    const [codeDiff, setCodeDiff] = useState<CodeDiff | undefined>();
    const [error, setError] = useState<string | undefined>();

    useEffect(() => {
        setLoading(true);
        setError(undefined);
        compareSourceToFile()
            .then(result => setCodeDiff(result.data))
            .catch((error) => setError(error))
            .finally(() => setLoading(false));
    }, [])

    return <div id={"code-source-compare"}>
        <PageHeader title={"Source Compare"} subtitle={"Compares the tables and stored procedures in the source database agains the local filesystem."}/>
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