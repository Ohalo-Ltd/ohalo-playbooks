/**
 * Utility functions for processing markdown content
 */

/**
 * Fix markdown links with spaces in URLs that aren't properly parsed.
 * Converts [text](#/d/file name.pdf) to [text](#/d/file%20name.pdf)
 * 
 * This handles cases where LLMs generate markdown links with spaces in the href,
 * which aren't properly parsed by the markdown renderer.
 */
export function fixMarkdownLinks(content: string): string {
  // Pattern to match [text](#/d/...anything including spaces...)
  // Uses negative lookahead to ensure we don't match already encoded URLs
  const pattern = /\[([^\]]+)\]\(#\/d\/([^)]+)\)/g;
  
  return content.replace(pattern, (match, linkText, docPath) => {
    // Check if the path contains spaces (indicating it needs encoding)
    if (docPath.includes(' ')) {
      // URL encode the spaces and other special characters
      const encodedPath = encodeURIComponent(docPath);
      return `[${linkText}](#/d/${encodedPath})`;
    }
    // Return as-is if no spaces
    return match;
  });
}
