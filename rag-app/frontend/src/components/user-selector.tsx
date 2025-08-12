"use client"

import * as React from "react"
import { ChevronDown, User } from "lucide-react"
import { cn } from "@/lib/utils"

interface User {
  email: string;
  name: string;
  role: string;
  groups: string[];
  employeeId: string;
}

const DEMO_USERS: User[] = [
  {
    email: "sarah.mitchell@enterprise.com",
    name: "Sarah Mitchell",
    role: "Project Manager",
    groups: ["pm", "management"],
    employeeId: "E99055",
  },
  {
    email: "charlie@corp.com",
    name: "Charlie Davis",
    role: "HR-Manager",
    groups: [
      "hr",
      "hr_manager",
      "can_see_all_reviews",
      "can_see_performance_reviews",
    ],
    employeeId: "E67890",
  },
];

interface UserSelectorProps {
  selectedUser: User;
  onUserChange: (user: User) => void;
}

export function UserSelector({
  selectedUser,
  onUserChange,
}: UserSelectorProps) {
  const [isOpen, setIsOpen] = React.useState(false);

  return (
    <div className="relative">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="flex items-center gap-3 px-4 py-3 bg-white border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors w-full text-left"
      >
        <div className="flex items-center justify-center w-8 h-8 bg-blue-100 rounded-full">
          <User className="w-4 h-4 text-blue-600" />
        </div>
        <div className="flex-1">
          <div className="font-medium text-gray-900">{selectedUser.name}</div>
          <div className="text-sm text-gray-500">{selectedUser.role}</div>
          <div className="text-xs text-gray-400">
            ID: {selectedUser.employeeId}
          </div>
        </div>
        <ChevronDown
          className={cn(
            "w-4 h-4 text-gray-400 transition-transform",
            isOpen && "rotate-180"
          )}
        />
      </button>

      {isOpen && (
        <div className="absolute top-full left-0 right-0 mt-2 bg-white border border-gray-200 rounded-lg shadow-lg z-10">
          <div className="py-2">
            {DEMO_USERS.map((user) => (
              <button
                key={user.email}
                onClick={() => {
                  onUserChange(user);
                  setIsOpen(false);
                }}
                className={cn(
                  "flex items-center gap-3 px-4 py-3 hover:bg-gray-50 transition-colors w-full text-left",
                  selectedUser.email === user.email && "bg-blue-50"
                )}
              >
                <div className="flex items-center justify-center w-8 h-8 bg-blue-100 rounded-full">
                  {/* Show initials instead of icon */}
                  <span className="font-bold text-gray-700">
                    {user.name
                      .split(" ")
                      .map((n) => n[0])
                      .join("")}
                  </span>
                </div>
                <div className="flex-1">
                  <div className="font-medium text-gray-900">{user.name}</div>
                  <div className="text-sm text-gray-500">{user.role}</div>
                  <div className="text-xs text-gray-400">
                    ID: {user.employeeId}
                  </div>
                  <div className="text-xs text-gray-400">
                    Groups: {user.groups.join(", ")}
                  </div>
                </div>
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

export { DEMO_USERS }
export type { User }
