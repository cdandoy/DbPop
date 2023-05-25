package org.dandoy.diff;

import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Function;

@SuppressWarnings("unused")
public class DiffLineGeneratorBuilder {
    boolean showInlineDiffs = false;
    boolean ignoreWhiteSpaces = false;
    boolean decompressDeltas = true;
    boolean reportLinesUnchanged = false;
    Function<String, List<String>> inlineDiffSplitter = DiffLineGenerator.SPLITTER_BY_CHARACTER;
    Function<String, String> lineNormalizer = Function.identity();
    BiPredicate<String, String> equalizer = null;

    DiffLineGeneratorBuilder() {
    }

    /**
     * Show inline diffs in generating diff rows or not.
     *
     * @param val the value to set. Default: false.
     * @return builder with configured showInlineDiff parameter
     */
    public DiffLineGeneratorBuilder showInlineDiffs(boolean val) {
        showInlineDiffs = val;
        return this;
    }

    /**
     * Ignore white spaces in generating diff rows or not.
     *
     * @param val the value to set. Default: true.
     * @return builder with configured ignoreWhiteSpaces parameter
     */
    public DiffLineGeneratorBuilder ignoreWhiteSpaces(boolean val) {
        ignoreWhiteSpaces = val;
        return this;
    }

    /**
     * Build the DiffLineGenerator. If some parameters is not set, the
     * default values are used.
     */
    public DiffLineGenerator build() {
        return new DiffLineGenerator(this);
    }

    /**
     * Deltas could be in a state, that would produce some unreasonable
     * results within an inline diff. So the deltas are decompressed into
     * smaller parts and rebuild. But this could result in more differences.
     */
    public DiffLineGeneratorBuilder decompressDeltas(boolean decompressDeltas) {
        this.decompressDeltas = decompressDeltas;
        return this;
    }

    /**
     * Per default each character is separatly processed. This variant
     * introduces processing by word, which does not deliver in word
     * changes. Therefore, the whole word will be tagged as changed:
     *
     * <pre>
     * false:    (aBa : aba) --  changed: a(B)a : a(b)a
     * true:     (aBa : aba) --  changed: (aBa) : (aba)
     * </pre>
     */
    public DiffLineGeneratorBuilder inlineDiffByWord(boolean inlineDiffByWord) {
        inlineDiffSplitter = inlineDiffByWord ? DiffLineGenerator.SPLITTER_BY_WORD : DiffLineGenerator.SPLITTER_BY_CHARACTER;
        return this;
    }

    /**
     * To provide some customized splitting a splitter can be provided. Here
     * someone could think about sentence splitter, comma splitter or stuff
     * like that.
     */
    public DiffLineGeneratorBuilder inlineDiffBySplitter(Function<String, List<String>> inlineDiffSplitter) {
        this.inlineDiffSplitter = inlineDiffSplitter;
        return this;
    }

    /**
     * By default, DiffLineGenerator preprocesses lines for HTML output. Tabs
     * and special HTML characters like "&lt;" are replaced with its encoded
     * value. To change this you can provide a customized line normalizer
     * here.
     */
    public DiffLineGeneratorBuilder lineNormalizer(Function<String, String> lineNormalizer) {
        this.lineNormalizer = lineNormalizer;
        return this;
    }

    /**
     * Provide an equalizer for diff processing.
     *
     * @param equalizer equalizer for diff processing.
     * @return builder with configured equalizer parameter
     */
    public DiffLineGeneratorBuilder equalizer(BiPredicate<String, String> equalizer) {
        this.equalizer = equalizer;
        return this;
    }
}
