/**
 * Styled document link component for ReactMarkdown
 */

'use client';

import { FileText, ExternalLink } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { apiClient } from "@/lib/api";

interface DocumentLinkProps {
  href?: string;
  children?: React.ReactNode;
  projectId?: string;
}

export function DocumentLink({ href, children, projectId }: DocumentLinkProps) {
  // Fetch project to get DXR URL
  const { data: project } = useQuery({
    queryKey: ["project", projectId],
    queryFn: () => apiClient.getProject(projectId!),
    enabled: !!projectId,
  });

  // Check if this is an internal document link (#/d/{doc_id})
  const isInternalDocLink = href?.startsWith("#/d/");
  
  // Transform internal links to DXR search URLs
  let transformedHref = href;
  if (isInternalDocLink && project?.dxr_url) {
    // Extract document ID/name from href and decode if URL encoded
    const docPath = href.substring(4); // Remove "#/d/"
    const docName = decodeURIComponent(docPath);
    
    // Build DXR search URL with file_name filter
    const filterQuery = { file_name: docName };
    const encodedQuery = encodeURIComponent(JSON.stringify(filterQuery));
    transformedHref = `${project.dxr_url.replace(/\/$/, '')}/search#query=${encodedQuery}`;
  }

  // Check if this is a DXR link (after transformation or already a DXR link)
  const isDxrLink = transformedHref?.includes("/search#query=");

  if (isDxrLink) {
    return (
      <a
        href={transformedHref}
        target="_blank"
        rel="noopener noreferrer"
        className="inline-flex items-center gap-1.5 px-1 py-0.5 my-1 border-b border-blue-800 dark:border-blue-400 rounded-sm transition-colors no-underline font-medium text-blue-800 dark:text-blue-300 bg-blue-50 hover:bg-blue-100 dark:bg-blue-950 dark:hover:bg-blue-800"
      >
        <FileText className="h-4 w-4 shrink-0" />
        <span className="text-sm">{children}</span>
        <ExternalLink className="h-3 w-3 shrink-0 ml-0.5" />
      </a>
    );
  }

  // Regular link
  return (
    <a
      href={transformedHref}
      target="_blank"
      rel="noopener noreferrer"
      className="text-blue-600 dark:text-blue-400 hover:underline"
    >
      {children}
    </a>
  );
}
