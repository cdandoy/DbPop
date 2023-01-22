import React, {useEffect, useState} from "react";
import {Button, Modal} from "react-bootstrap";
import {TableName, tableNameToFqName} from "../../models/TableName";
import {ForeignKey} from "../../models/ForeignKey";
import tableApi from "../../api/tableApi";

export default function SelectTableDependenciesComponent({tableName, close}: {
    tableName: TableName | undefined,
    close: () => void
}) {
    const [foreignKeys, setForeignKeys] = useState<ForeignKey[]>([])
    useEffect(() => {
        if (tableName) {
            tableApi(tableName)
                .then(result => {
                    setForeignKeys(result.data.foreignKeys);
                })
        } else {
            setForeignKeys([])
        }
    }, [tableName]);

    return <>
        {tableName &&
            <Modal show={!!tableName} onHide={() => close()} size={"lg"}>
                <Modal.Header closeButton>
                    <Modal.Title style={{fontFamily: "monospace"}}>{tableName.table}</Modal.Title>
                </Modal.Header>
                <Modal.Body>
                    <>
                        <p>Table {tableName.table} has two required dependencies</p>
                        <div className={"ms-3"}>
                            {foreignKeys.length === 0 && <><i className={"fa fa-spinner fa-spin"}/> Loading</>}
                            {foreignKeys.map(foreignKey => (
                                <div key={tableNameToFqName(foreignKey.pkTableName)} className={"mb-3"} style={{fontFamily: "monospace"}}>
                                    <div>CONSTRAINT {foreignKey.name} FOREIGN KEY</div>
                                    <div className={"ms-3"}>
                                        {foreignKey.fkColumns.length == 1 && foreignKey.fkColumns.join(', ')}
                                        {foreignKey.fkColumns.length > 1 && '(' + foreignKey.fkColumns.join(', ') + ')'}
                                        &nbsp;
                                        REFERENCES {foreignKey.pkTableName.table}
                                        (
                                        {foreignKey.pkColumns.join(', ')}
                                        )
                                    </div>
                                </div>
                            ))}
                        </div>
                    </>
                </Modal.Body>
                <Modal.Footer>
                    <Button variant="primary" onClick={() => close()}>
                        Close
                    </Button>
                </Modal.Footer>
            </Modal>
        }
    </>
}