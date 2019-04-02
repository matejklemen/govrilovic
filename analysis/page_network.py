import pandas as pd
import networkx as nx

if __name__ == "__main__":
    df_sites = pd.read_csv("../crawl_9_sites/site.csv", delimiter=';')
    df_sites.columns = ['site_id', 'site_name', 'robots', 'sitemap']

    # {259: 'e-prostor.gov.si', 260: 'www.e-prostor.gov.si', 261: 'kpv.gov.si', ...
    sites = pd.Series(df_sites.site_name.values,
                      index=df_sites.site_id).to_dict()

    """
    Graph generation code.

    df_pages = pd.read_csv(
        "../crawl_9_sites/page_noContent.csv", delimiter=";")
    df_pages.columns = ['id', 'site_id', 'type',
                        'lsh_hash', 'url', 'status_code', 'time_retreived']

    pages_by_site_id = df_pages.groupby('site_id').agg(
        {'id': lambda x: x.tolist()})['id'].to_dict()

    g = nx.DiGraph(pages_by_site_id)
    nx.write_pajek(g, "./pages.net")

    """
    g = nx.read_pajek("./pages.net")

    nodes_by_degree = sorted(g.degree, key=lambda x: x[1], reverse=True)

    max_degree_node = nodes_by_degree[0]

    print("Number of nodes in graph:", g.number_of_nodes())
    print("Number of links in graph:", g.number_of_edges())
    print("Average degree:", sum(d for n, d in g.degree()) /
          float(g.number_of_nodes()))

    print("Max degree node: ",
          sites[int(float(max_degree_node[0]))], " | Degree: ", max_degree_node[1])
