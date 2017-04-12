#include <iostream>
#include <stdio.h>
#include <fstream>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <algorithm>
#include <iterator>

using namespace std;
static int maxlevel = 0;
#define log 0

enum NodeColor
{
    red = 0,
    black
};

struct TNode
{
    unsigned int id;
    unsigned int counts;
    struct TNode *parent;
    NodeColor color;
    struct TNode* left;
    struct TNode* right;
};

class EventCounter
{
private:
    ifstream in_stream;
    struct TNode* rootRBT; /* Stores the root of the Red Black tree */

public:
    EventCounter(const string &); /* Opens the input file */
    void CloseFile(); /* Closes input file */
    void OpenCommandFile(const string &); /* Opens command file */
    unsigned int GetElementListCount();
    void UpdateRoot(TNode* root);
    struct TNode* SortedFileToRBTRecur(unsigned int n, int level);
    TNode* InsertNode(unsigned int id_ins, unsigned int counts_ins);
    void InsertFix(TNode* t);
    void LeftRotate(TNode *p);
    void RightRotate(TNode *p);
    void DelNode(unsigned int id_del);
    void DelFix(TNode* t);
    TNode* Successor(TNode *p);
    bool ExecuteCommand(string rdLine);
    void ParseCommand();
    void Count(unsigned int id_c);
    void InRange(unsigned int id_r1, unsigned int id_r2);
    void Next(unsigned int id_next);
    void Previous(unsigned int id_prev);
    void Increase(unsigned int id_inc, unsigned int counts_inc);
    void Reduce(unsigned int id_red, unsigned int counts_red);
    TNode* SearchNode(unsigned int id_s);
};

/* It opens the file identified by argument filename and associate it with the stream object 'in_stream' */
EventCounter::EventCounter(const string &fileName)
{
    in_stream.open(fileName.c_str());
    if (!in_stream.is_open())
    {
        cout<<"Error opening file: "<<fileName<<std::endl;
        exit(1);
    }
    else
    {
        //cout<<"File opened successfully"<<endl;
    }
}

/* The function closes the file associated with the stream object. */
void EventCounter::CloseFile()
{
    in_stream.close();
}

void EventCounter::OpenCommandFile(const string &fileName)
{
    in_stream.open(fileName.c_str());
    if (!in_stream.is_open())
    {
        cout<<"Error opening command file: "<<fileName<<std::endl;
        exit(1);
    }
    else
    {
        //cout<<"Command File opened successfully"<<endl;
    }
}

/* The function reads the first line of a file and return the element list count.
   Called only once */
unsigned int EventCounter::GetElementListCount()
{
    unsigned int lc;
    in_stream >> lc;
    return lc;
}

/* This function creates a new node and updates its values */
struct TNode* newNode(unsigned int id, unsigned int counts, int level)
{
    struct TNode* pNode = (struct TNode*)malloc(sizeof(struct TNode));
    if(pNode == NULL)
    {
        cout<<"Malloc returned NULL"<<endl;
        exit(0);
    }
    pNode->id = id;
    pNode->counts = counts;
    if((maxlevel - level) % 2 == 0)
    {
        pNode->color = black;
    }
    else
    {
        pNode->color = red;
    }

    if(level == 0) //For root is always going to be black
    {
        pNode->color = black;
    }

    pNode->left = NULL;
    pNode->right = NULL;
    pNode->parent = NULL;

    return pNode;
}

/* Function creates the 'Red and Black tree' recursively and return the root node of the tree.
 * The function recursively calls the left subtree followed by the root and then the right
 * sub-tree in inorder fashion and creates the node corresponding to the loose ith index binding.*/
struct TNode* EventCounter::SortedFileToRBTRecur( unsigned int n, int level)
{
    unsigned int id_cur;
    unsigned int counts_cur;

    /* Base Case */
    if (n <= 0)
        return NULL;

    struct TNode *left = SortedFileToRBTRecur((n)/2, level+1);

    in_stream >> id_cur >> counts_cur;

    struct TNode *root = newNode(id_cur, counts_cur, level); /* Create new nnode */
    if(left)
    {
        left->parent = root; //Update parent
    }
    root->left = left;

    struct TNode *right = SortedFileToRBTRecur(n-(n)/2-1, level+1);
    if(right)
    {
        right->parent = root; //update parent
    }
    root->right = right;

    return root;
}

/* Creates a node and places it in its relevant position by traversing through the child nodes. */
TNode* EventCounter::InsertNode(unsigned int id_ins, unsigned int counts_ins)
{
    TNode *p,*q;

    struct TNode* t = (struct TNode*)malloc(sizeof(struct TNode));
    if(t == NULL)
    {
        cout<<"Malloc returned NULL"<<endl;
        exit(0);
    }

    t->id = id_ins;
    t->counts = counts_ins;
    t->left = NULL;
    t->right = NULL;
    t->color = red; /* Color of the inserted node should always be red */
#if log
    cout<<"Node inserted - "<<t->id<<endl;
#endif
    p = rootRBT;
    q = NULL;

    if(rootRBT == NULL)
    {
        rootRBT = t;
        t->parent = NULL;
    }
    else
    {
        while(p != NULL) /* Traversal to find position */
        {
            q = p;
            if(p->id < t->id)
                p = p->right;
            else
                p = p->left;
        }
        t->parent = q;
        if(q->id < t->id)
            q->right = t;
        else
            q->left = t;
    }
    InsertFix(t); /* Fix property violation */
    return t;
}

/* This function helps to fix violation */
void EventCounter::InsertFix(TNode* t)
{
    TNode* u = NULL; //u: uncle
    if(rootRBT == t)
    {
        t->color = black;
        return;
    }
    while(t->parent != NULL && t->parent->color == red)
    {
        TNode* g = t->parent->parent; // g: grandparent
        if(g->left == t->parent)
        {
            if(g->right != NULL) /* Fix when uncle is there by recoloring */
            {
                u = g->right;
                if(u->color == red)
                {
                    t->parent->color = black;
                    u->color = black;
                    g->color = red;
                    t = g; /* The new node for property violation fix is the grandparent */
                }
            }
            else /* Fix when uncle node is absent by rotation */
            {
                if(t->parent->right == t)
                {
                    t = t->parent;
                    LeftRotate(t);
                }
                t->parent->color = black;
                g->color = red;
                RightRotate(g);
            }
        }
        else
        {
            if(g->left != NULL) /* Fix when uncle is there by recoloring */
            {
                u = g->left;
                if(u->color == red)
                {
                    t->parent->color = black;
                    u->color = black;
                    g->color = red;
                    t = g; /* The new node for property violation fix is the grandparent */
                }
            }
            else /* Fix when uncle node is absent by rotation */
            {
                if(t->parent->left == t)
                {
                    t = t->parent;
                    RightRotate(t);
                }
                t->parent->color = black;
                g->color = red;
                LeftRotate(g);
            }
        }
        rootRBT->color = black;
    }
}

/* Rotate left */
void EventCounter::LeftRotate(TNode *p)
{
    if(p->right == NULL)
        return ;
    else
    {
        TNode* y = p->right;
        if(y->left != NULL)
        {
            p->right = y->left;
            y->left->parent = p;
        }
        else
            p->right = NULL;
        if(p->parent != NULL)
            y->parent = p->parent;
        if(p->parent == NULL)
            rootRBT = y;
        else
        {
            if(p == p->parent->left)
                p->parent->left = y;
            else
                p->parent->right = y;
        }
        y->left = p;
        p->parent = y;
    }
}

/* Rotate right */
void EventCounter::RightRotate(TNode* p)
{
    if(p->left == NULL)
        return ;
    else
    {
        TNode* y = p->left;
        if(y->right != NULL)
        {
            p->left = y->right;
            y->right->parent = p;
        }
        else
            p->left = NULL;
        if(p->parent != NULL)
            y->parent = p->parent;
        if(p->parent == NULL)
            rootRBT = y;
        else
        {
            if(p == p->parent->left)
                p->parent->left = y;
            else
                p->parent->right = y;
        }
        y->right = p;
        p->parent = y;
    }
}

/* Removes the node from its position and frees the memory location. */
void EventCounter::DelNode(unsigned int id_del)
{
    if(rootRBT == NULL)
    {
        return;
    }

    TNode* p = rootRBT;
    TNode* y = NULL;
    TNode* q = NULL;
    int found = 0;
    while(p!=NULL && found == 0) /* Traversal to find the node */
    {
        if(p->id == id_del)
            found = 1;
        if(found == 0)
        {
            if(p->id < id_del)
                p = p->right;
            else
                p = p->left;
        }
    }
    if(found == 0)
    {
        return ;
    }
    else
    {
#if log
        cout<<"\nDeleted Element: "<<p->id;
#endif

        if(p->left == NULL || p->right == NULL)
            y = p;
        else
            y = Successor(p);
        if(y->left != NULL)
            q = y->left;
        else
        {
            if(y->right != NULL)
                q = y->right;
            else
                q = NULL;
        }
        if(q != NULL)
            q->parent = y->parent;
        if(y->parent == NULL)
            rootRBT = q;
        else
        {
            if(y == y->parent->left)
                y->parent->left = q;
            else
                y->parent->right = q;
        }
        if(y != p)
        {
            p->color = y->color;
            p->id = y->id;
            p->counts = y->counts;
        }

        if(y->color == black)
            DelFix(q); /* Fix RB Tree property violation due to deletion */
    }
}

/* The function searches for the node with id = id_s. Returns the respective node pointer if found or returns null. */
TNode* EventCounter::SearchNode(unsigned int id_s)
{
    if(rootRBT == NULL)
    {
        return  NULL;
    }
    TNode* p = rootRBT;
    int found = 0;
    while(p != NULL && found == 0) /* Tree traversal */
    {
        if(p->id == id_s)
            found = 1;
        if(found == 0)
        {
            if(p->id < id_s)
                p = p->right;
            else
                p = p->left;
        }
    }
    if(found == 0)
    {
        return NULL;
    }
    else
    {
#if log
        cout<<"\n Id: "<<p->id;
#endif
        return p;
    }
}

/* Fixes Property violation after deletion */
void EventCounter::DelFix(TNode *p)
{
    TNode* s; /* s: sibling */
    while(p != rootRBT && p->color == black) //A black node deletion can result into two red nodes with a parent-child relationship
    {
        if(p->parent->left == p)
        {
            s = p->parent->right;
            if(s->color == red) /* When sibling has different color - Fix by coloring sibling black and left-rotation inside if */
            {
                s->color = black;
                p->parent->color = red;
                LeftRotate(p->parent);
                s = p->parent->right;
            }
            if(s->right->color == black && s->left->color == black)
            {
                s->color = red; /* Recolor to maintain black height */
                p = p->parent;
            }
            else
            {
                if(s->right->color == black) /* Fix sibling's children  */
                {
                    s->left->color = black;
                    s->color = red;
                    RightRotate(s);
                    s = p->parent->right;
                }
                s->color = p->parent->color;
                p->parent->color = black;
                s->right->color = black;
                LeftRotate(p->parent);
                p = rootRBT;
            }
        }
        else
        {
            s = p->parent->left;
            if(s->color == red) /* When sibling has different color - Fix by coloring sibling black and left-rotation inside if */
            {
                s->color = black;
                p->parent->color = red;
                RightRotate(p->parent);
                s = p->parent->left;
            }
            if(s->left->color == black && s->right->color == black)
            {
                s->color = red; /* Recolor to maintain black height */
                p = p->parent;
            }
            else
            {
                if(s->left->color == black) /* Fix sibling's children  */
                {
                    s->right->color = black;
                    s->color = red;
                    LeftRotate(s);
                    s = p->parent->left;
                }
                s->color = p->parent->color;
                p->parent->color = black;
                s->left->color = black;
                RightRotate(p->parent);
                p = rootRBT;
            }
        }
        p->color = black;
        rootRBT->color = black;
    }
}

/* Returns the successor to  a given node */
TNode* EventCounter::Successor(TNode *z)
{
    if(z->right!=NULL)
    {
        z=z->right;
        while(z->left!=NULL)
            z=z->left;
        return z;
    }
 
    TNode* y = z->parent;
    while(y!=NULL && z==y->right)
    {
        z=y;
        y=y->parent;
    }
    return y;
}

/* Increase count for id = id_inc by counts_inc */
void EventCounter::Increase(unsigned int id_inc, unsigned int counts_inc)
{
    TNode* pNode = SearchNode(id_inc);

    if(pNode)
    {
        //Update
        pNode->counts += counts_inc;
    }
    else
    {
        pNode = InsertNode(id_inc, counts_inc);
    }

    cout<<pNode->counts<<endl;
}

/* Reduces count for  id = id_red by counts_red */
void EventCounter::Reduce(unsigned int id_red, unsigned int counts_red)
{
    TNode* pNode = SearchNode(id_red);
    if(pNode)
    {
        //Update
        if(pNode->counts <= counts_red)
        {
            DelNode(id_red);
        }
        else
        {
            pNode->counts -= counts_red;
#if log
            cout<<"Reduce:"<<pNode->counts<<endl;
#endif
            cout<<pNode->counts<<endl;

            return;
        }
    }
    cout<<0<<endl;
}

bool isequals(const string& a, const string& b, int sz)
{
    if (b.size() != sz)
        return false;
    for (unsigned int i = 0; i < sz; ++i)
    {
        //if (tolower(a[i]) != tolower(b[i]))
        if ((a[i]) != (b[i]))
            return false;
    }
    return true;
}

/* Displays the count for node id = id_c*/
void EventCounter::Count(unsigned int id_c)
{

    TNode* pNode = SearchNode(id_c);
    if(pNode)
    {
        cout<<pNode->counts<<endl;
    }
    else
    {
        cout<<"0"<<endl;
    }
}

/* Displays the next element to node with id = id_next */
void EventCounter::Next(unsigned int id_next)
{
    TNode* root = rootRBT;
    if(root == NULL)
    {
        cout<<"No tree";
        return;
    }
    TNode* temp = NULL; /* Stores the best updated Next element */
    while(root)
    {
        if(id_next < root->id)
        {
            root = root->left;
            if(root && root->id > id_next )
            {
                if(!temp || (temp && temp->id > root->id))
                {
                    temp = root;
                }
            }
        }
        else if(id_next >= root->id)
        {
            root = root->right;
            if(root && root->id > id_next)
            {
                if(!temp || (temp && temp->id > root->id))
                {
                    temp = root;
                }
            }
        }
    }

    if(temp == NULL)
    {
        cout<<"0 0"<<endl;
    }
    else
    {
        cout<<(temp->id)<<" "<<(temp->counts)<<endl;
    }
}

/* Displays the previous element to node with id = id_prev */
void EventCounter::Previous(unsigned int id_prev)
{
    TNode* root = rootRBT;
    if(root == NULL)
    {
        cout<<"No tree";
        return;
    }
    TNode* temp = NULL; /* Stors the best updated previous element */
    while(root)
    {
        if(id_prev <= root->id)
        {
            root = root->left;
            if(root && root->id < id_prev )
            {
                if(!temp || (temp && temp->id < root->id))
                {
                    temp = root;
                }
            }
        }
        else if(id_prev > root->id)
        {
            root = root->right;
            if(root && root->id < id_prev)
            {
                if(!temp || (temp && temp->id < root->id  ))
                {
                    temp = root;
                }
            }
        }
    }

    if(temp == NULL)
    {
        cout<<"0 0"<<endl;
    }
    else
    {
        cout<<(temp->id)<<" "<<(temp->counts)<<endl;
    }
}

void calcCount(TNode* root, unsigned int* totalcount, unsigned int id_r1, unsigned int id_r2) /* Calculates total count recursively between id_r1 and id_r2 */
{
    if ( NULL == root )
        return;

    if ( id_r1 < root->id )
        calcCount(root->left, totalcount, id_r1, id_r2);

    if ( id_r1 <= root->id && id_r2 >= root->id )
        *totalcount += root->counts;

    if ( id_r2 > root->id )
        calcCount(root->right, totalcount, id_r1, id_r2);
}

/* Dispays the total count of ids in the range [id_r1, id_r2] */
void EventCounter::InRange(unsigned int id_r1, unsigned int id_r2)
{
    TNode* root = rootRBT;

    unsigned int totalcount = 0;
    calcCount(root, &totalcount, id_r1, id_r2);
    cout<<totalcount<<endl;
}

bool EventCounter::ExecuteCommand(string rdLine)
{
    char* token;
    unsigned int param[2]; /* max 2 parameter */
    char fnIns[50] = ""; /* Function Instance to invoke */

    token = strtok((char*)rdLine.c_str(), " ");
    strncpy(fnIns, token, strlen(token) + 1);

    int i = 0;
    while( token != NULL && i < 3)
    {
        if(i != 0)
        {
            param[i-1] = atoi(token);
        }
        i++;
        token = strtok(NULL, " ");
    }

    /* Compares the Function instance to be invoked */
    if(isequals(fnIns, "increase", strlen("increase")) == true)
    {
        Increase(param[0], param[1]);
    }
    else if(isequals(fnIns, "reduce", strlen("reduce")) == true)
    {
        Reduce(param[0], param[1]);
    }
    else if(isequals(fnIns, "count", strlen("count")) == true)
    {
        Count(param[0]);
    }
    else if(isequals(fnIns, "inrange", strlen("inrange")) == true)
    {
        InRange(param[0], param[1]);
    }
    else if(isequals(fnIns, "next", strlen("next")) == true)
    {
        Next(param[0]);
    }
    else if(isequals(fnIns, "previous", strlen("previous")) == true)
    {
        Previous(param[0]);
    }
    else if(isequals(fnIns, "quit", strlen("quit")) == true)
    {
        return false;
    }
    return true;
}


void EventCounter::ParseCommand()
{
    string rdLine = "";
    bool ret = true;

    while(!in_stream.eof() && ret)
    {
        getline(in_stream, rdLine);
        ret = ExecuteCommand(rdLine);
    }
}

void EventCounter::UpdateRoot(TNode* upd)
{
    rootRBT = upd;
}

int main(int argc, char* args[])
{
    //string fileName = "test_100000000.txt";
    //string fileName = "test_100.txt";
    //string fileName = "test_1000000.txt";
    //string cmdFileName = "commands.txt";
    string fileName = "";
    string cmdFileName = "";

    for(int i = 0; i < argc; i++)
    {
        if(i == 1)
        {
            fileName = args[i];

        }
        if(i == 2)
        {
            cmdFileName = args[i];

        }
    }


    if(fileName == "")
    {
        cout<<"Enter Filename"<<endl;
        exit(0);
    }
    EventCounter* ec = new EventCounter(fileName);

    unsigned int element_count = ec->GetElementListCount(); //Call this function once only
    maxlevel = ceil(log2(element_count + 1)); //Helps in coloring of the RB Tree

    struct TNode *root = ec->SortedFileToRBTRecur(element_count, 0);
    
    ec->UpdateRoot(root);
    ec->CloseFile();
    if(cmdFileName == "")
    {
        string rdLine = "";
        while(isequals(rdLine, "quit", strlen("quit")) == false)
        {
            getline(std::cin, rdLine);
            ec->ExecuteCommand(rdLine);
        }
    }
    else
    {
        ec->OpenCommandFile(cmdFileName);
        ec->ParseCommand();
        ec->CloseFile();
    }

    return 1;
}
