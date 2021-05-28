import argparse

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-a','--Action', type=str, help = "Type of actions", metavar = '', choices=['create-cluster', 
                                                                                      'list-clusters',
                                                                                      'terminate-cluster',
                                                                                      'add_steps',
                                                                                      'execute_steps'])


    # Create cluster
    parser.add_argument('-cname','--Cname', type=str, help = "Name Cluster", metavar = '')
    parser.add_argument('-config','--Config File', type=str, help = "File with the fonfiguration of the emr cluster", metavar = '')

    # List clusters
    parser.add_argument('-lc','--List of clusters', type=str, help = "List of Cluster", metavar = '')

    # Terminate cluster
    parser.add_argument('-idc','--Cluster id', type=str, help = "Id of the cluster", metavar = '')

    # add steps to the cluster
    parser.add_argument('-steps_file','--Config file of the steps', type=str, help = "Add steps from json file", metavar = '')

    # execute steps in clusters
    parser.add_argument('-execute_steps','--Execute steps in cluster', type=str, help = "execute steps involved to the clusters", metavar = '')    


    args = parser.parse_args()

    if args.Action == 'create-cluster':
        print(args.cname)
    elif args.Action == 'list-clusters':
    elif args.Action == 'terminate-cluster':
    elif args.Action == 'add_steps':
    elif args.Action == 'execute_steps':
    else:
        print("Action is invalid")
    

    # if args.demo_type == 'short-lived':
    #     demo_short_lived_cluster()
    # elif args.demo_type == 'long-lived':
    #     demo_long_lived_cluster()