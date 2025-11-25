/**
 * User switcher component for testing entitlements in chat.
 */

'use client';

import * as React from 'react';
import { Check, ChevronsUpDown, UserCircle } from 'lucide-react';
import { cn } from '@/lib/utils';
import { Button } from '@/components/ui/button';
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from '@/components/ui/command';
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover';

export interface User {
  email: string;
  name: string;
}

interface UserSwitcherProps {
  users: User[];
  currentUser: User | null;
  onUserChange: (user: User | null) => void;
  enabled?: boolean;
}

export function UserSwitcher({
  users,
  currentUser,
  onUserChange,
  enabled = true,
}: UserSwitcherProps) {
  const [open, setOpen] = React.useState(false);

  if (!enabled || users.length === 0) {
    return null;
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          role="combobox"
          aria-expanded={open}
          className="w-[200px] justify-between"
          size="sm"
        >
          <UserCircle className="mr-2 h-4 w-4 shrink-0" />
          <span className="truncate">
            {currentUser?.name || users[0]?.name || 'Select user'}
          </span>
          <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-[200px] p-0">
        <Command>
          <CommandInput placeholder="Search users..." />
          <CommandList>
            <CommandEmpty>No user found.</CommandEmpty>
            <CommandGroup>
              {users.map((user) => (
                <CommandItem
                  key={user.email}
                  value={user.email}
                  onSelect={() => {
                    onUserChange(user);
                    setOpen(false);
                  }}
                >
                  <Check
                    className={cn(
                      'mr-2 h-4 w-4',
                      currentUser?.email === user.email
                        ? 'opacity-100'
                        : 'opacity-0'
                    )}
                  />
                  <div className="flex flex-col">
                    <span className="font-medium">{user.name}</span>
                    <span className="text-xs text-muted-foreground">
                      {user.email}
                    </span>
                  </div>
                </CommandItem>
              ))}
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}
