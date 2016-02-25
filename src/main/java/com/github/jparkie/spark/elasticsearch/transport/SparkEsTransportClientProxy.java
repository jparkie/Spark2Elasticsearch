package com.github.jparkie.spark.elasticsearch.transport;

import com.github.jparkie.spark.elasticsearch.util.SparkEsException;
import org.elasticsearch.action.*;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.exists.ExistsRequest;
import org.elasticsearch.action.exists.ExistsRequestBuilder;
import org.elasticsearch.action.exists.ExistsResponse;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainRequestBuilder;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.fieldstats.FieldStatsRequest;
import org.elasticsearch.action.fieldstats.FieldStatsRequestBuilder;
import org.elasticsearch.action.fieldstats.FieldStatsResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptRequestBuilder;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptResponse;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptRequestBuilder;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptResponse;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptRequestBuilder;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptResponse;
import org.elasticsearch.action.percolate.*;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.suggest.SuggestRequest;
import org.elasticsearch.action.suggest.SuggestRequestBuilder;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.action.termvectors.*;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.Headers;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * Restrict access to TransportClient by disabling close() without use of SparkEsTransportClientManager.
 *
 * Java Class because of a Scala Self Reflective Type issue.
 *
 * TODO: Replace class with Scala implementation.
 */
public class SparkEsTransportClientProxy implements Client {
    private final TransportClient internalTransportClient;

    public SparkEsTransportClientProxy(TransportClient transportClient) {
        this.internalTransportClient = transportClient;
    }

    @Override
    public AdminClient admin() {
        return this.internalTransportClient.admin();
    }

    @Override
    public ActionFuture<IndexResponse> index(IndexRequest request) {
        return this.internalTransportClient.index(request);
    }

    @Override
    public void index(IndexRequest request, ActionListener<IndexResponse> listener) {
        this.internalTransportClient.index(request, listener);
    }

    @Override
    public IndexRequestBuilder prepareIndex() {
        return this.internalTransportClient.prepareIndex();
    }

    @Override
    public ActionFuture<UpdateResponse> update(UpdateRequest request) {
        return this.internalTransportClient.update(request);
    }

    @Override
    public void update(UpdateRequest request, ActionListener<UpdateResponse> listener) {
        this.internalTransportClient.update(request, listener);
    }

    @Override
    public UpdateRequestBuilder prepareUpdate() {
        return this.internalTransportClient.prepareUpdate();
    }

    @Override
    public UpdateRequestBuilder prepareUpdate(String index, String type, String id) {
        return this.internalTransportClient.prepareUpdate(index, type, id);
    }

    @Override
    public IndexRequestBuilder prepareIndex(String index, String type) {
        return this.internalTransportClient.prepareIndex(index, type);
    }

    @Override
    public IndexRequestBuilder prepareIndex(String index, String type, @Nullable String id) {
        return this.internalTransportClient.prepareIndex(index, type, id);
    }

    @Override
    public ActionFuture<DeleteResponse> delete(DeleteRequest request) {
        return this.internalTransportClient.delete(request);
    }

    @Override
    public void delete(DeleteRequest request, ActionListener<DeleteResponse> listener) {
        this.internalTransportClient.delete(request, listener);
    }

    @Override
    public DeleteRequestBuilder prepareDelete() {
        return this.internalTransportClient.prepareDelete();
    }

    @Override
    public DeleteRequestBuilder prepareDelete(String index, String type, String id) {
        return this.internalTransportClient.prepareDelete(index, type, id);
    }

    @Override
    public ActionFuture<BulkResponse> bulk(BulkRequest request) {
        return this.internalTransportClient.bulk(request);
    }

    @Override
    public void bulk(BulkRequest request, ActionListener<BulkResponse> listener) {
        this.internalTransportClient.bulk(request, listener);
    }

    @Override
    public BulkRequestBuilder prepareBulk() {
        return this.internalTransportClient.prepareBulk();
    }

    @Override
    public ActionFuture<GetResponse> get(GetRequest request) {
        return this.internalTransportClient.get(request);
    }

    @Override
    public void get(GetRequest request, ActionListener<GetResponse> listener) {
        this.internalTransportClient.get(request, listener);
    }

    @Override
    public GetRequestBuilder prepareGet() {
        return this.internalTransportClient.prepareGet();
    }

    @Override
    public GetRequestBuilder prepareGet(String index, @Nullable String type, String id) {
        return this.internalTransportClient.prepareGet(index, type, id);
    }

    @Override
    public PutIndexedScriptRequestBuilder preparePutIndexedScript() {
        return this.internalTransportClient.preparePutIndexedScript();
    }

    @Override
    public PutIndexedScriptRequestBuilder preparePutIndexedScript(@Nullable String scriptLang, String id, String source) {
        return this.internalTransportClient.preparePutIndexedScript(scriptLang, id, source);
    }

    @Override
    public void deleteIndexedScript(DeleteIndexedScriptRequest request, ActionListener<DeleteIndexedScriptResponse> listener) {
        this.internalTransportClient.deleteIndexedScript(request, listener);
    }

    @Override
    public ActionFuture<DeleteIndexedScriptResponse> deleteIndexedScript(DeleteIndexedScriptRequest request) {
        return this.internalTransportClient.deleteIndexedScript(request);
    }

    @Override
    public DeleteIndexedScriptRequestBuilder prepareDeleteIndexedScript() {
        return this.internalTransportClient.prepareDeleteIndexedScript();
    }

    @Override
    public DeleteIndexedScriptRequestBuilder prepareDeleteIndexedScript(@Nullable String scriptLang, String id) {
        return this.internalTransportClient.prepareDeleteIndexedScript(scriptLang, id);
    }

    @Override
    public void putIndexedScript(PutIndexedScriptRequest request, ActionListener<PutIndexedScriptResponse> listener) {
        this.internalTransportClient.putIndexedScript(request, listener);
    }

    @Override
    public ActionFuture<PutIndexedScriptResponse> putIndexedScript(PutIndexedScriptRequest request) {
        return this.internalTransportClient.putIndexedScript(request);
    }

    @Override
    public GetIndexedScriptRequestBuilder prepareGetIndexedScript() {
        return this.internalTransportClient.prepareGetIndexedScript();
    }

    @Override
    public GetIndexedScriptRequestBuilder prepareGetIndexedScript(@Nullable String scriptLang, String id) {
        return this.internalTransportClient.prepareGetIndexedScript(scriptLang, id);
    }

    @Override
    public void getIndexedScript(GetIndexedScriptRequest request, ActionListener<GetIndexedScriptResponse> listener) {
        this.internalTransportClient.getIndexedScript(request, listener);
    }

    @Override
    public ActionFuture<GetIndexedScriptResponse> getIndexedScript(GetIndexedScriptRequest request) {
        return this.internalTransportClient.getIndexedScript(request);
    }

    @Override
    public ActionFuture<MultiGetResponse> multiGet(MultiGetRequest request) {
        return this.internalTransportClient.multiGet(request);
    }

    @Override
    public void multiGet(MultiGetRequest request, ActionListener<MultiGetResponse> listener) {
        this.internalTransportClient.multiGet(request, listener);
    }

    @Override
    public MultiGetRequestBuilder prepareMultiGet() {
        return this.internalTransportClient.prepareMultiGet();
    }

    @Override
    public ActionFuture<CountResponse> count(CountRequest request) {
        return this.internalTransportClient.count(request);
    }

    @Override
    public void count(CountRequest request, ActionListener<CountResponse> listener) {
        this.internalTransportClient.count(request, listener);
    }

    @Override
    public CountRequestBuilder prepareCount(String... indices) {
        return this.internalTransportClient.prepareCount(indices);
    }

    @Override
    public ActionFuture<ExistsResponse> exists(ExistsRequest request) {
        return this.internalTransportClient.exists(request);
    }

    @Override
    public void exists(ExistsRequest request, ActionListener<ExistsResponse> listener) {
        this.internalTransportClient.exists(request, listener);
    }

    @Override
    public ExistsRequestBuilder prepareExists(String... indices) {
        return this.internalTransportClient.prepareExists(indices);
    }

    @Override
    public ActionFuture<SuggestResponse> suggest(SuggestRequest request) {
        return this.internalTransportClient.suggest(request);
    }

    @Override
    public void suggest(SuggestRequest request, ActionListener<SuggestResponse> listener) {
        this.internalTransportClient.suggest(request, listener);
    }

    @Override
    public SuggestRequestBuilder prepareSuggest(String... indices) {
        return this.internalTransportClient.prepareSuggest(indices);
    }

    @Override
    public ActionFuture<SearchResponse> search(SearchRequest request) {
        return this.internalTransportClient.search(request);
    }

    @Override
    public void search(SearchRequest request, ActionListener<SearchResponse> listener) {
        this.internalTransportClient.search(request, listener);
    }

    @Override
    public SearchRequestBuilder prepareSearch(String... indices) {
        return this.internalTransportClient.prepareSearch(indices);
    }

    @Override
    public ActionFuture<SearchResponse> searchScroll(SearchScrollRequest request) {
        return this.internalTransportClient.searchScroll(request);
    }

    @Override
    public void searchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
        this.internalTransportClient.searchScroll(request, listener);
    }

    @Override
    public SearchScrollRequestBuilder prepareSearchScroll(String scrollId) {
        return this.internalTransportClient.prepareSearchScroll(scrollId);
    }

    @Override
    public ActionFuture<MultiSearchResponse> multiSearch(MultiSearchRequest request) {
        return this.internalTransportClient.multiSearch(request);
    }

    @Override
    public void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
        this.internalTransportClient.multiSearch(request, listener);
    }

    @Override
    public MultiSearchRequestBuilder prepareMultiSearch() {
        return this.internalTransportClient.prepareMultiSearch();
    }

    @Override
    public ActionFuture<TermVectorsResponse> termVectors(TermVectorsRequest request) {
        return this.internalTransportClient.termVectors(request);
    }

    @Override
    public void termVectors(TermVectorsRequest request, ActionListener<TermVectorsResponse> listener) {
        this.internalTransportClient.termVectors(request, listener);
    }

    @Override
    public TermVectorsRequestBuilder prepareTermVectors() {
        return this.internalTransportClient.prepareTermVectors();
    }

    @Override
    public TermVectorsRequestBuilder prepareTermVectors(String index, String type, String id) {
        return this.internalTransportClient.prepareTermVectors(index, type, id);
    }

    @Override
    public ActionFuture<TermVectorsResponse> termVector(TermVectorsRequest request) {
        return this.internalTransportClient.termVector(request);
    }

    @Override
    public void termVector(TermVectorsRequest request, ActionListener<TermVectorsResponse> listener) {
        this.internalTransportClient.termVector(request, listener);
    }

    @Override
    public TermVectorsRequestBuilder prepareTermVector() {
        return this.internalTransportClient.prepareTermVector();
    }

    @Override
    public TermVectorsRequestBuilder prepareTermVector(String index, String type, String id) {
        return this.internalTransportClient.prepareTermVector(index, type, id);
    }

    @Override
    public ActionFuture<MultiTermVectorsResponse> multiTermVectors(MultiTermVectorsRequest request) {
        return this.internalTransportClient.multiTermVectors(request);
    }

    @Override
    public void multiTermVectors(MultiTermVectorsRequest request, ActionListener<MultiTermVectorsResponse> listener) {
        this.internalTransportClient.multiTermVectors(request, listener);
    }

    @Override
    public MultiTermVectorsRequestBuilder prepareMultiTermVectors() {
        return this.internalTransportClient.prepareMultiTermVectors();
    }

    @Override
    public ActionFuture<PercolateResponse> percolate(PercolateRequest request) {
        return this.internalTransportClient.percolate(request);
    }

    @Override
    public void percolate(PercolateRequest request, ActionListener<PercolateResponse> listener) {
        this.internalTransportClient.percolate(request, listener);
    }

    @Override
    public PercolateRequestBuilder preparePercolate() {
        return this.internalTransportClient.preparePercolate();
    }

    @Override
    public ActionFuture<MultiPercolateResponse> multiPercolate(MultiPercolateRequest request) {
        return this.internalTransportClient.multiPercolate(request);
    }

    @Override
    public void multiPercolate(MultiPercolateRequest request, ActionListener<MultiPercolateResponse> listener) {
        this.internalTransportClient.multiPercolate(request, listener);
    }

    @Override
    public MultiPercolateRequestBuilder prepareMultiPercolate() {
        return this.internalTransportClient.prepareMultiPercolate();
    }

    @Override
    public ExplainRequestBuilder prepareExplain(String index, String type, String id) {
        return this.internalTransportClient.prepareExplain(index, type, id);
    }

    @Override
    public ActionFuture<ExplainResponse> explain(ExplainRequest request) {
        return this.internalTransportClient.explain(request);
    }

    @Override
    public void explain(ExplainRequest request, ActionListener<ExplainResponse> listener) {
        this.internalTransportClient.explain(request, listener);
    }

    @Override
    public ClearScrollRequestBuilder prepareClearScroll() {
        return this.internalTransportClient.prepareClearScroll();
    }

    @Override
    public ActionFuture<ClearScrollResponse> clearScroll(ClearScrollRequest request) {
        return this.internalTransportClient.clearScroll(request);
    }

    @Override
    public void clearScroll(ClearScrollRequest request, ActionListener<ClearScrollResponse> listener) {
        this.internalTransportClient.clearScroll(request, listener);
    }

    @Override
    public FieldStatsRequestBuilder prepareFieldStats() {
        return this.internalTransportClient.prepareFieldStats();
    }

    @Override
    public ActionFuture<FieldStatsResponse> fieldStats(FieldStatsRequest request) {
        return this.internalTransportClient.fieldStats(request);
    }

    @Override
    public void fieldStats(FieldStatsRequest request, ActionListener<FieldStatsResponse> listener) {
        this.internalTransportClient.fieldStats(request, listener);
    }

    @Override
    public Settings settings() {
        return this.internalTransportClient.settings();
    }

    @Override
    public Headers headers() {
        return this.internalTransportClient.headers();
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> ActionFuture<Response> execute(Action<Request, Response, RequestBuilder> action, Request request) {
        return this.internalTransportClient.execute(action, request);
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void execute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
        this.internalTransportClient.execute(action, request, listener);
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> RequestBuilder prepareExecute(Action<Request, Response, RequestBuilder> action) {
        return this.internalTransportClient.prepareExecute(action);
    }

    @Override
    public ThreadPool threadPool() {
        return this.internalTransportClient.threadPool();
    }

    @Override
    public void close() {
        throw new SparkEsException("close() is not supported in SparkEsTransportClientProxy. Please close with SparkEsTransportClientManager.");
    }
}
