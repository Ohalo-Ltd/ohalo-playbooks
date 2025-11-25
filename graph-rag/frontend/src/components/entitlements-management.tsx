/**
 * Entitlements management component for users and groups.
 */

'use client';

import * as React from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Plus, Trash2, Edit2, Users as UsersIcon, Shield } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { apiClient, ProjectUser, ProjectGroup } from '@/lib/api';
import { toast } from 'sonner';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';

interface EntitlementsManagementProps {
  projectId: string;
}

export function EntitlementsManagement({ projectId }: EntitlementsManagementProps) {
  const queryClient = useQueryClient();
  const [userDialogOpen, setUserDialogOpen] = React.useState(false);
  const [groupDialogOpen, setGroupDialogOpen] = React.useState(false);
  const [editingUser, setEditingUser] = React.useState<ProjectUser | null>(null);
  const [editingGroup, setEditingGroup] = React.useState<ProjectGroup | null>(null);

  // User form state
  const [userEmail, setUserEmail] = React.useState('');
  const [userName, setUserName] = React.useState('');
  const [userIdpId, setUserIdpId] = React.useState('');

  // Group form state
  const [groupCode, setGroupCode] = React.useState('');
  const [groupName, setGroupName] = React.useState('');

  // Fetch users
  const { data: users = [], isLoading: loadingUsers } = useQuery({
    queryKey: ['project-users', projectId],
    queryFn: () => apiClient.listProjectUsers(projectId),
  });

  // Fetch groups
  const { data: groups = [], isLoading: loadingGroups } = useQuery({
    queryKey: ['project-groups', projectId],
    queryFn: () => apiClient.listProjectGroups(projectId),
  });

  // User mutations
  const createUserMutation = useMutation({
    mutationFn: () =>
      apiClient.createProjectUser(projectId, {
        email: userEmail,
        name: userName,
        idp_id: userIdpId || undefined,
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['project-users', projectId] });
      toast.success('User created successfully');
      setUserDialogOpen(false);
      resetUserForm();
    },
    onError: (error: Error) => {
      toast.error(`Failed to create user: ${error.message}`);
    },
  });

  const updateUserMutation = useMutation({
    mutationFn: () =>
      apiClient.updateProjectUser(projectId, editingUser!.id, {
        email: userEmail,
        name: userName,
        idp_id: userIdpId || undefined,
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['project-users', projectId] });
      toast.success('User updated successfully');
      setUserDialogOpen(false);
      resetUserForm();
    },
    onError: (error: Error) => {
      toast.error(`Failed to update user: ${error.message}`);
    },
  });

  const deleteUserMutation = useMutation({
    mutationFn: (userId: string) => apiClient.deleteProjectUser(projectId, userId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['project-users', projectId] });
      toast.success('User deleted successfully');
    },
    onError: (error: Error) => {
      toast.error(`Failed to delete user: ${error.message}`);
    },
  });

  // Group mutations
  const createGroupMutation = useMutation({
    mutationFn: () =>
      apiClient.createProjectGroup(projectId, {
        code: groupCode,
        name: groupName,
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['project-groups', projectId] });
      toast.success('Group created successfully');
      setGroupDialogOpen(false);
      resetGroupForm();
    },
    onError: (error: Error) => {
      toast.error(`Failed to create group: ${error.message}`);
    },
  });

  const updateGroupMutation = useMutation({
    mutationFn: () =>
      apiClient.updateProjectGroup(projectId, editingGroup!.id, {
        code: groupCode,
        name: groupName,
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['project-groups', projectId] });
      toast.success('Group updated successfully');
      setGroupDialogOpen(false);
      resetGroupForm();
    },
    onError: (error: Error) => {
      toast.error(`Failed to update group: ${error.message}`);
    },
  });

  const deleteGroupMutation = useMutation({
    mutationFn: (groupId: string) => apiClient.deleteProjectGroup(projectId, groupId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['project-groups', projectId] });
      toast.success('Group deleted successfully');
    },
    onError: (error: Error) => {
      toast.error(`Failed to delete group: ${error.message}`);
    },
  });

  const resetUserForm = () => {
    setUserEmail('');
    setUserName('');
    setUserIdpId('');
    setEditingUser(null);
  };

  const resetGroupForm = () => {
    setGroupCode('');
    setGroupName('');
    setEditingGroup(null);
  };

  const handleEditUser = (user: ProjectUser) => {
    setEditingUser(user);
    setUserEmail(user.email);
    setUserName(user.name);
    setUserIdpId(user.idp_id || '');
    setUserDialogOpen(true);
  };

  const handleEditGroup = (group: ProjectGroup) => {
    setEditingGroup(group);
    setGroupCode(group.code);
    setGroupName(group.name);
    setGroupDialogOpen(true);
  };

  const handleSaveUser = () => {
    if (editingUser) {
      updateUserMutation.mutate();
    } else {
      createUserMutation.mutate();
    }
  };

  const handleSaveGroup = () => {
    if (editingGroup) {
      updateGroupMutation.mutate();
    } else {
      createGroupMutation.mutate();
    }
  };

  return (
    <div className="space-y-6">
      <Tabs defaultValue="users" className="w-full">
        <TabsList>
          <TabsTrigger value="users">
            <UsersIcon className="h-4 w-4 mr-2" />
            Users
          </TabsTrigger>
          <TabsTrigger value="groups">
            <Shield className="h-4 w-4 mr-2" />
            Groups
          </TabsTrigger>
        </TabsList>

        <TabsContent value="users" className="space-y-4 mt-6">
          <div className="flex items-center justify-between">
            <div>
              <h3 className="text-lg font-semibold">Users</h3>
              <p className="text-sm text-muted-foreground">
                Manage user profiles for entitlements
              </p>
            </div>
            <Button
              onClick={() => {
                resetUserForm();
                setUserDialogOpen(true);
              }}
              size="sm"
            >
              <Plus className="h-4 w-4 mr-2" />
              Add User
            </Button>
          </div>

          <div className="border rounded-lg">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Name</TableHead>
                  <TableHead>Email</TableHead>
                  <TableHead>IdP ID</TableHead>
                  <TableHead className="text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {loadingUsers ? (
                  <TableRow>
                    <TableCell colSpan={4} className="text-center py-8 text-muted-foreground">
                      Loading users...
                    </TableCell>
                  </TableRow>
                ) : users.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={4} className="text-center py-8 text-muted-foreground">
                      No users yet. Add your first user to get started.
                    </TableCell>
                  </TableRow>
                ) : (
                  users.map((user) => (
                    <TableRow key={user.id}>
                      <TableCell className="font-medium">{user.name}</TableCell>
                      <TableCell>{user.email}</TableCell>
                      <TableCell className="text-muted-foreground">
                        {user.idp_id || '-'}
                      </TableCell>
                      <TableCell className="text-right">
                        <div className="flex justify-end gap-2">
                          <Button
                            variant="ghost"
                            size="icon"
                            onClick={() => handleEditUser(user)}
                          >
                            <Edit2 className="h-4 w-4" />
                          </Button>
                          <Button
                            variant="ghost"
                            size="icon"
                            onClick={() => deleteUserMutation.mutate(user.id)}
                          >
                            <Trash2 className="h-4 w-4 text-destructive" />
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </div>
        </TabsContent>

        <TabsContent value="groups" className="space-y-4 mt-6">
          <div className="flex items-center justify-between">
            <div>
              <h3 className="text-lg font-semibold">Groups</h3>
              <p className="text-sm text-muted-foreground">
                Manage groups for role-based access control
              </p>
            </div>
            <Button
              onClick={() => {
                resetGroupForm();
                setGroupDialogOpen(true);
              }}
              size="sm"
            >
              <Plus className="h-4 w-4 mr-2" />
              Add Group
            </Button>
          </div>

          <div className="border rounded-lg">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Name</TableHead>
                  <TableHead>Code</TableHead>
                  <TableHead className="text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {loadingGroups ? (
                  <TableRow>
                    <TableCell colSpan={3} className="text-center py-8 text-muted-foreground">
                      Loading groups...
                    </TableCell>
                  </TableRow>
                ) : groups.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={3} className="text-center py-8 text-muted-foreground">
                      No groups yet. Add your first group to get started.
                    </TableCell>
                  </TableRow>
                ) : (
                  groups.map((group) => (
                    <TableRow key={group.id}>
                      <TableCell className="font-medium">{group.name}</TableCell>
                      <TableCell className="font-mono text-sm">{group.code}</TableCell>
                      <TableCell className="text-right">
                        <div className="flex justify-end gap-2">
                          <Button
                            variant="ghost"
                            size="icon"
                            onClick={() => handleEditGroup(group)}
                          >
                            <Edit2 className="h-4 w-4" />
                          </Button>
                          <Button
                            variant="ghost"
                            size="icon"
                            onClick={() => deleteGroupMutation.mutate(group.id)}
                          >
                            <Trash2 className="h-4 w-4 text-destructive" />
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </div>
        </TabsContent>
      </Tabs>

      {/* User Dialog */}
      <Dialog open={userDialogOpen} onOpenChange={setUserDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{editingUser ? 'Edit User' : 'Add User'}</DialogTitle>
            <DialogDescription>
              {editingUser
                ? 'Update user information for entitlements.'
                : 'Add a new user to manage document access.'}
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-4 py-4">
            <div className="grid gap-2">
              <Label htmlFor="user-name">Name</Label>
              <Input
                id="user-name"
                value={userName}
                onChange={(e) => setUserName(e.target.value)}
                placeholder="John Doe"
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="user-email">Email</Label>
              <Input
                id="user-email"
                type="email"
                value={userEmail}
                onChange={(e) => setUserEmail(e.target.value)}
                placeholder="john@example.com"
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="user-idp-id">IdP ID (Optional)</Label>
              <Input
                id="user-idp-id"
                value={userIdpId}
                onChange={(e) => setUserIdpId(e.target.value)}
                placeholder="user_123abc"
              />
              <p className="text-xs text-muted-foreground">
                Identity provider user ID for SSO integration
              </p>
            </div>
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                setUserDialogOpen(false);
                resetUserForm();
              }}
            >
              Cancel
            </Button>
            <Button
              onClick={handleSaveUser}
              disabled={
                !userName ||
                !userEmail ||
                createUserMutation.isPending ||
                updateUserMutation.isPending
              }
            >
              {editingUser ? 'Update' : 'Create'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Group Dialog */}
      <Dialog open={groupDialogOpen} onOpenChange={setGroupDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{editingGroup ? 'Edit Group' : 'Add Group'}</DialogTitle>
            <DialogDescription>
              {editingGroup
                ? 'Update group information.'
                : 'Add a new group for role-based access control.'}
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-4 py-4">
            <div className="grid gap-2">
              <Label htmlFor="group-name">Name</Label>
              <Input
                id="group-name"
                value={groupName}
                onChange={(e) => setGroupName(e.target.value)}
                placeholder="Engineering Team"
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="group-code">Code</Label>
              <Input
                id="group-code"
                value={groupCode}
                onChange={(e) => setGroupCode(e.target.value)}
                placeholder="engineering"
              />
              <p className="text-xs text-muted-foreground">
                Unique identifier for this group (e.g., engineering, hr, finance)
              </p>
            </div>
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                setGroupDialogOpen(false);
                resetGroupForm();
              }}
            >
              Cancel
            </Button>
            <Button
              onClick={handleSaveGroup}
              disabled={
                !groupName ||
                !groupCode ||
                createGroupMutation.isPending ||
                updateGroupMutation.isPending
              }
            >
              {editingGroup ? 'Update' : 'Create'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
