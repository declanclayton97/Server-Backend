import pkg from "docusign-esign";
const {
  ApiClient,
  EnvelopesApi,
  EnvelopeDefinition,
  Document,
  Signer,
  Recipients,
  Tabs,
  SignHere,
  InitialHere,
} = pkg;

class DocuSignService {
  constructor() {
    this.apiClient = new ApiClient();
    this.apiClient.setBasePath(
      process.env.DOCUSIGN_BASE_PATH || "https://demo.docusign.net/restapi"
    );

    this.accountId = process.env.DOCUSIGN_ACCOUNT_ID;
    this.integrationKey = process.env.DOCUSIGN_INTEGRATION_KEY;
    this.userId = process.env.DOCUSIGN_USER_ID;
    this.privateKey = process.env.DOCUSIGN_PRIVATE_KEY;
  }

  async authenticate() {
    const jwtLifeSec = 3600;
    const scopes = ["signature", "impersonation"];

    const results = await this.apiClient.requestJWTUserToken(
      this.integrationKey,
      this.userId,
      scopes,
      this.privateKey,
      jwtLifeSec
    );

    this.apiClient.addDefaultHeader(
      "Authorization",
      "Bearer " + results.body.access_token
    );
    return results;
  }

  async createEnvelopeWithSignatureFields(
    pdfBytes,
    recipientEmail,
    recipientName,
    signaturePositions
  ) {
    await this.authenticate();

    const envelopesApi = new EnvelopesApi(this.apiClient);

    const envelopeDefinition = new EnvelopeDefinition();
    envelopeDefinition.emailSubject = "Please approve the mockup sheet";
    envelopeDefinition.status = "sent";

    const doc = new Document();
    const base64Doc = Buffer.from(pdfBytes).toString("base64");
    doc.documentBase64 = base64Doc;
    doc.name = "Mockup Sheet";
    doc.fileExtension = "pdf";
    doc.documentId = "1";
    envelopeDefinition.documents = [doc];

    const signer = new Signer();
    signer.email = recipientEmail;
    signer.name = recipientName;
    signer.recipientId = "1";
    signer.routingOrder = "1";

    const signHereTabs = signaturePositions.map((pos, index) => {
      const signHere = new SignHere();
      signHere.documentId = "1";
      signHere.pageNumber = pos.page.toString();
      signHere.xPosition = pos.x.toString();
      signHere.yPosition = pos.y.toString();
      signHere.tabLabel = `Logo_Approval_${index + 1}`;
      signHere.optional = "false";
      return signHere;
    });

    const initialTabs = signaturePositions.map((pos, index) => {
      const initial = new InitialHere();
      initial.documentId = "1";
      initial.pageNumber = pos.page.toString();
      initial.xPosition = (pos.x + 150).toString();
      initial.yPosition = pos.y.toString();
      initial.tabLabel = `Initial_${index + 1}`;
      initial.optional = "true";
      return initial;
    });

    const tabs = new Tabs();
    tabs.signHereTabs = signHereTabs;
    tabs.initialHereTabs = initialTabs;
    signer.tabs = tabs;

    const recipients = new Recipients();
    recipients.signers = [signer];
    envelopeDefinition.recipients = recipients;

    const results = await envelopesApi.createEnvelope(this.accountId, {
      envelopeDefinition: envelopeDefinition,
    });

    return results;
  }
}

export default DocuSignService;
