import useWebSocket, {ReadyState} from "react-use-websocket";
import React, {useEffect, useState} from "react";

interface ConnectionStatus {
    configured: boolean;
    errorMessage?: string;
}

interface StructuredWebSocketMessage {
    sourceConnectionStatus: ConnectionStatus;
    targetConnectionStatus: ConnectionStatus;
    hasCode: boolean;
    codeChanges: number;
    hasCodeDiffs: boolean;
    codeDiffChanges: number;
}

export interface WebSocketState {
    hasSource: boolean;
    sourceErrorMessage?: string;
    hasTarget: boolean;
    targetErrorMessage?: string;
    codeChanged: number;
    hasCodeDiffs: boolean;
    codeDiffChanges: number;
    connected: boolean;
    hasCode: boolean;
}

export const WebSocketStateContext = React.createContext<WebSocketState>({
    hasSource: false,
    hasTarget: false,
    codeChanged: 0,
    hasCodeDiffs: false,
    codeDiffChanges: 0,
    connected: false,
    hasCode: false,
});

function getWebsocketUrl(): string {
    const documentUrl = document.URL;
    const url = new URL(documentUrl);
    const hostname = url.hostname;
    const port = url.port;
    if (hostname === 'localhost' && port === '3000') {
        return 'ws://localhost:8080/ws/site'
    } else {
        return `ws://${hostname}:${port}/ws/site`
    }
}

export function useWebSocketState(): WebSocketState {
    const [hasSource, setHasSource] = useState(false);
    const [sourceErrorMessage, setSourceErrorMessage] = useState<string | undefined>();
    const [hasTarget, setHasTarget] = useState(false);
    const [targetErrorMessage, setTargetErrorMessage] = useState<string | undefined>();
    const [codeChanged, setCodeChanged] = useState(0);
    const [hasCodeDiffs, setHasCodeDiffs] = useState(false);
    const [codeDiffChanges, setCodeDiffChanges] = useState(0);
    const [connected, setConnected] = useState(false);
    const [hasCode, setHasCode] = useState(false);
    const {lastJsonMessage, readyState} = useWebSocket(
        getWebsocketUrl(),
        {
            shouldReconnect: () => true,
            // reconnectAttempts: 10,
            reconnectInterval: (attemptNumber: number) =>
                Math.min(Math.pow(2, attemptNumber) * 1000, 10000),
        }
    );

    useEffect(() => {
        if (lastJsonMessage) {
            const message = lastJsonMessage as any as StructuredWebSocketMessage;
            setHasSource(message.sourceConnectionStatus.configured);
            setSourceErrorMessage(message.sourceConnectionStatus.errorMessage);
            setHasTarget(message.targetConnectionStatus.configured);
            setTargetErrorMessage(message.targetConnectionStatus.errorMessage);
            setCodeChanged(message.codeChanges);
            setHasCodeDiffs(message.hasCodeDiffs);
            setCodeDiffChanges(message.codeDiffChanges);
            setHasCode(message.hasCode);
        }
    }, [lastJsonMessage]);


    const connectionStatus = {
        [ReadyState.CONNECTING]: false,
        [ReadyState.OPEN]: true,
        [ReadyState.CLOSING]: false,
        [ReadyState.CLOSED]: false,
        [ReadyState.UNINSTANTIATED]: false,
    }[readyState];

    useEffect(() => {
        setConnected(connectionStatus);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [readyState]);

    return {
        hasSource,
        sourceErrorMessage,
        hasTarget,
        targetErrorMessage,
        codeChanged,
        hasCodeDiffs,
        codeDiffChanges,
        connected,
        hasCode
    }
}
