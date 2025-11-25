"use client"

import * as React from "react"
import { Check, ChevronsUpDown, Plus, Settings } from "lucide-react"

import { Button } from "@/components/ui/button"
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandSeparator,
} from "@/components/ui/command"
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover"
import { cn } from "@/lib/utils"
import { useQuery } from "@tanstack/react-query"
import { apiClient } from "@/lib/api"

interface ProjectSwitcherProps {
  currentProjectId?: string
  onProjectChange?: (projectId: string) => void
  onOpenSettings?: (projectId: string) => void
  onCreateProject?: () => void
}

export function ProjectSwitcher({
  currentProjectId,
  onProjectChange,
  onOpenSettings,
  onCreateProject,
}: ProjectSwitcherProps) {
  const [open, setOpen] = React.useState(false)

  const { data: projects = [], isLoading } = useQuery({
    queryKey: ["projects"],
    queryFn: () => apiClient.listProjects(),
  })

  const currentProject = projects.find((p) => p.id === currentProjectId)

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          role="combobox"
          aria-expanded={open}
          className="w-[200px] justify-between"
        >
          {currentProject ? currentProject.name : "Select project..."}
          <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-[200px] p-0">
        <Command>
          <CommandInput placeholder="Search projects..." />
          <CommandList>
            <CommandEmpty>No project found.</CommandEmpty>
            <CommandGroup>
              {projects.map((project) => (
                <CommandItem
                  key={project.id}
                  value={project.name}
                  onSelect={() => {
                    onProjectChange?.(project.id);
                    setOpen(false);
                  }}
                  className="flex items-center justify-between group"
                >
                  <div className="flex items-center">
                    <Check
                      className={cn(
                        "mr-2 h-4 w-4",
                        currentProjectId === project.id
                          ? "opacity-100"
                          : "opacity-0"
                      )}
                    />
                    {project.name}
                  </div>
                  <Button
                    variant="ghost"
                    size="icon"
                    className="h-6 w-6 opacity-0 group-hover:opacity-100"
                    onClick={(e) => {
                      e.stopPropagation();
                      onOpenSettings?.(project.id);
                      setOpen(false);
                    }}
                  >
                    <Settings className="h-3 w-3" />
                    <span className="sr-only">Project settings</span>
                  </Button>
                </CommandItem>
              ))}
            </CommandGroup>
            <CommandSeparator />
            <CommandGroup>
              <CommandItem
                onSelect={() => {
                  onCreateProject?.();
                  setOpen(false);
                }}
              >
                <Plus className="mr-2 h-4 w-4" />
                Create Project
              </CommandItem>
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}
