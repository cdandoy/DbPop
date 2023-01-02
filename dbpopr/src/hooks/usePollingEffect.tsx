import {useEffect, useRef} from 'react';

export function usePollingEffect(
    callback: (...arg: unknown[]) => Promise<boolean>,
    options: { interval: number } = {interval: 3000}
) {
    const timeoutRef: any = useRef(null);

    useEffect(() => {
        ;(async function pollingFn() {
            let continuePolling = await callback();
            if (continuePolling) {
                timeoutRef.current = setTimeout(pollingFn, options.interval)
            }
        })()
        return () => clearTimeout(timeoutRef.current)
    }, [ callback, options.interval])
}
