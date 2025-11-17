import pkg from 'docusign-esign';
const { ApiClient, EnvelopesApi, EnvelopeDefinition, Document, Signer, Recipients, Tabs, SignHere, InitialHere } = pkg;

class DocuSignService {
  constructor() {
    this.apiClient = new ApiClient();

    // PRODUCTION EU REGION - Use environment variable
    this.basePath = process.env.DOCUSIGN_BASE_PATH || 'https://demo.docusign.net/restapi';
    this.apiClient.setBasePath(this.basePath);

    this.accountId = process.env.DOCUSIGN_ACCOUNT_ID;
    this.integrationKey = process.env.DOCUSIGN_INTEGRATION_KEY;
    this.userId = process.env.DOCUSIGN_USER_ID;

    // Fix private key formatting
    let privateKey = process.env.DOCUSIGN_PRIVATE_KEY;
    if (privateKey && privateKey.includes('\\n')) {
      privateKey = privateKey.replace(/\\n/g, '\n');
    }
    this.privateKey = privateKey;

    console.log('🔧 DocuSign Service initialized - Version: 2025-01-17-v4');
    console.log('  - Base Path:', this.basePath);
    console.log('  - Account ID:', this.accountId);
    console.log('  - Integration Key:', this.integrationKey);
    console.log('  - User ID:', this.userId);
    console.log('  - Private Key exists:', !!this.privateKey);
  }

  async authenticate() {
    try {
      const jwtLifeSec = 3600;
      const scopes = ['signature', 'impersonation'];
      
      const results = await this.apiClient.requestJWTUserToken(
        this.integrationKey,
        this.userId,
        scopes,
        this.privateKey,
        jwtLifeSec
      );
      
      // Get the access token
      const accessToken = results.body.access_token;

      // Set the auth header
      this.apiClient.addDefaultHeader('Authorization', 'Bearer ' + accessToken);

      console.log('✅ Authenticated successfully');
      console.log('  - Using base URI:', this.apiClient.basePath);
      console.log('  - Account ID:', this.accountId);
      
      return results;
    } catch (error) {
      console.error('Authentication error:', error.message);
      throw error;
    }
  }

  async createEnvelopeWithSignatureFields(pdfBytes, recipientEmail, recipientName, signaturePositions) {
    await this.authenticate();
    
    const envelopesApi = new EnvelopesApi(this.apiClient);
    
    const envelopeDefinition = new EnvelopeDefinition();
    envelopeDefinition.emailSubject = 'Please approve the mockup sheet';
    envelopeDefinition.status = 'sent';
    
    const doc = new Document();
    const base64Doc = Buffer.from(pdfBytes).toString('base64');
    doc.documentBase64 = base64Doc;
    doc.name = 'Mockup Sheet';
    doc.fileExtension = 'pdf';
    doc.documentId = '1';
    envelopeDefinition.documents = [doc];
    
    const signer = new Signer();
    signer.email = recipientEmail;
    signer.name = recipientName;
    signer.recipientId = '1';
    signer.routingOrder = '1';
    
    const signHereTabs = signaturePositions.map((pos, index) => {
      const signHere = new SignHere();
      signHere.documentId = '1';
      signHere.pageNumber = pos.page.toString();
      signHere.xPosition = pos.x.toString();
      signHere.yPosition = pos.y.toString();
      signHere.tabLabel = `Logo_Approval_${index + 1}`;
      return signHere;
    });
    
    const tabs = new Tabs();
    tabs.signHereTabs = signHereTabs;
    signer.tabs = tabs;
    
    const recipients = new Recipients();
    recipients.signers = [signer];
    envelopeDefinition.recipients = recipients;
    
    console.log('Sending envelope to account:', this.accountId);
    
    const results = await envelopesApi.createEnvelope(this.accountId, {
      envelopeDefinition: envelopeDefinition
    });
    
    return results;
  }
}

export default DocuSignService;
