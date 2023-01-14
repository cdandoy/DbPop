export function toHumanReadableSize(bytes: number) {
    let unit = bytes === 1 ? 'byte' : 'bytes';
    let size = bytes;
    if (size > 1024) {
        size = Math.round(size / 1024);
        unit = "Kb";
    }
    if (size > 1024) {
        size = Math.round(size / 1024);
        unit = "Mb";
    }
    return {
        size: size,
        unit: unit,
        text: `${size.toLocaleString()} ${unit}`
    }
}

export function Plural(count: number, text: string) {
    if (count === 1) return `1 ${text}`
    if (text.endsWith('y')) return `${count} ${text.substring(0, text.length - 1)}ies`;
    return `${count} ${text}s`
}
