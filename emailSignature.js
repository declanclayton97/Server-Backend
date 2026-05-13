// Shared Tuff Workwear email signature. Previously inlined inside the
// proof-chase module — extracted here so the order tracking pipeline can
// reuse the same block by exposing it as a {{signature}} variable.
//
// EMAIL_ASSETS_BASE_URL points at the hosted /email-assets/ folder on
// Render where the logo + social icons live. Defaults to the public
// backend URL so dev deploys without overriding the env var still render
// the images correctly.

const EMAIL_ASSETS_BASE =
  process.env.EMAIL_ASSETS_BASE_URL ||
  "https://server-backend-1i47.onrender.com/email-assets";

const SIGNATURE_TEXT = `--

Tuff Workwear Ltd
Tuffshop.co.uk
144-146 Aberford Road, Leeds, LS26 8LG

Follow us:
Facebook: https://facebook.com/tuffshop.co.uk/
Twitter: https://twitter.com/tuffshop
LinkedIn: https://www.linkedin.com/company/tuff-workwear-ltd
YouTube: https://youtube.com/channel/UCjI55--M7y8397npIA-BAdw
Instagram: https://instagram.com/tuff.shop/
Pinterest: https://pinterest.com/tuffshop/

This email and any attachments to it may be confidential and are intended solely for the use of the individual to whom it is addressed. Any views or opinions expressed are solely those of the author and do not necessarily represent those of Tuff Workwear Ltd.
If you are not the intended recipient of this email, you must neither take any action based upon its contents, nor copy or show it to anyone.
Please contact the sender if you believe you have received this email in error.`;

const SIGNATURE_HTML = `<table cellpadding="0" cellspacing="0" border="0" style="margin-top:24px;font-family:'Lucida Grande',Arial,Helvetica,sans-serif;font-size:12px;color:#222;">
  <tr>
    <td style="padding:6px 0;">
      <a href="https://tuffshop.co.uk" style="text-decoration:none;border:0;">
        <img src="${EMAIL_ASSETS_BASE}/image001.png" alt="Tuff Workwear" width="113" height="80" style="display:block;border:0;outline:none;">
      </a>
    </td>
  </tr>
  <tr>
    <td style="padding:6px 0;">
      <a href="https://maps.apple.com/?q=144-146+Aberford+Road,Leeds,LS26+8LG" style="color:#222;text-decoration:none;">144-146 Aberford Road, Leeds, LS26 8LG</a>
    </td>
  </tr>
  <tr>
    <td style="padding:6px 0;">
      <table cellpadding="0" cellspacing="0" border="0">
        <tr>
          <td style="padding:0 6px 0 0;"><a href="https://facebook.com/tuffshop.co.uk/" style="border:0;"><img src="${EMAIL_ASSETS_BASE}/image002.png" alt="Facebook" width="32" height="32" style="display:block;border:0;outline:none;"></a></td>
          <td style="padding:0 6px;"><a href="https://twitter.com/tuffshop?lang=en" style="border:0;"><img src="${EMAIL_ASSETS_BASE}/image003.png" alt="Twitter" width="32" height="32" style="display:block;border:0;outline:none;"></a></td>
          <td style="padding:0 6px;"><a href="https://www.linkedin.com/company/tuff-workwear-ltd" style="border:0;"><img src="${EMAIL_ASSETS_BASE}/image004.png" alt="LinkedIn" width="32" height="32" style="display:block;border:0;outline:none;"></a></td>
          <td style="padding:0 6px;"><a href="https://youtube.com/channel/UCjI55--M7y8397npIA-BAdw" style="border:0;"><img src="${EMAIL_ASSETS_BASE}/image005.png" alt="YouTube" width="32" height="32" style="display:block;border:0;outline:none;"></a></td>
          <td style="padding:0 6px;"><a href="https://instagram.com/tuff.shop/?hl=en" style="border:0;"><img src="${EMAIL_ASSETS_BASE}/image006.png" alt="Instagram" width="32" height="32" style="display:block;border:0;outline:none;"></a></td>
          <td style="padding:0 6px;"><a href="https://pinterest.com/tuffshop/" style="border:0;"><img src="${EMAIL_ASSETS_BASE}/image007.png" alt="Pinterest" width="32" height="32" style="display:block;border:0;outline:none;"></a></td>
          <td style="padding:0 6px;"><a href="https://wa.me/" style="border:0;"><img src="${EMAIL_ASSETS_BASE}/image008.png" alt="WhatsApp" width="32" height="32" style="display:block;border:0;outline:none;"></a></td>
          <td style="padding:0 0 0 6px;"><a href="https://www.google.com/maps/place/Tuffshop.co.uk/@53.7529091,-1.4490469,17z/data=!3m1!4b1!4m5!3m4!1s0x48795d81c9d2d0df:0x51b05d91e1ea6be9!8m2!3d53.7529091!4d-1.4468582" style="border:0;"><img src="${EMAIL_ASSETS_BASE}/image009.png" alt="Google" width="32" height="32" style="display:block;border:0;outline:none;"></a></td>
        </tr>
      </table>
    </td>
  </tr>
  <tr>
    <td style="padding:6px 0;font-size:12px;">To follow us on social media or leave a review click the icons - we would love your feedback!</td>
  </tr>
</table>
<p style="color:#888;font-family:'Lucida Grande',Arial,Helvetica,sans-serif;font-size:11px;line-height:1.4;margin-top:18px;">
  This email and any attachments to it may be confidential and are intended solely for the use of the individual to whom it is addressed. Any views or opinions expressed are solely those of the author and do not necessarily represent those of Tuff Workwear Ltd.<br>
  If you are not the intended recipient of this email, you must neither take any action based upon its contents, nor copy or show it to anyone.<br>
  Please contact the sender if you believe you have received this email in error.
</p>`;

export { EMAIL_ASSETS_BASE, SIGNATURE_TEXT, SIGNATURE_HTML };
